# marbledb.pxd - C++ declarations for MarbleDB
# cython: language_level=3

from libc.stdint cimport int64_t, uint64_t
from libc.stddef cimport size_t
from libcpp.string cimport string
from libcpp.memory cimport unique_ptr, shared_ptr
from libcpp.vector cimport vector
from libcpp cimport bool
from libcpp.functional cimport function

# Arrow C++ declarations
from pyarrow.includes.libarrow cimport (
    CSchema,
    CRecordBatch,
    CTable,
)

# MarbleDB C++ declarations
cdef extern from "marble/status.h" namespace "marble":
    cdef cppclass Status:
        Status()
        bool ok()
        string ToString()
        @staticmethod
        Status OK()

cdef extern from "marble/record.h" namespace "marble":
    cdef cppclass Schema:
        pass

    cdef cppclass Key:
        pass

    cdef cppclass Int64Key(Key):
        Int64Key(int64_t value)

    cdef cppclass Record:
        pass

cdef extern from "marble/query.h" namespace "marble":
    cdef cppclass QueryResult:
        bool HasNext() const
        Status Next(shared_ptr[CRecordBatch]* batch)
        shared_ptr[CSchema] schema() const
        int64_t num_rows() const
        int64_t num_batches() const

cdef extern from "marble/table.h" namespace "marble":
    cdef cppclass TableSchema:
        pass

cdef extern from "marble/column_family.h" namespace "marble":
    cdef cppclass ColumnFamilyHandle:
        pass

    cdef cppclass ColumnFamilyOptions:
        ColumnFamilyOptions()
        bool enable_bloom_filter

    cdef cppclass ColumnFamilyDescriptor:
        ColumnFamilyDescriptor(const string& name, const ColumnFamilyOptions& options)

cdef extern from "marble/db.h" namespace "marble":
    cdef cppclass DBOptions:
        DBOptions()
        string db_path
        size_t memtable_size_threshold
        size_t sstable_size_threshold
        size_t block_size
        bool enable_wal
        size_t wal_buffer_size
        size_t max_level0_files
        size_t max_level_files_base
        size_t level_multiplier
        bool enable_sparse_index
        size_t index_granularity
        size_t target_block_size
        bool enable_bloom_filter
        size_t bloom_filter_bits_per_key
        bool enable_hot_key_cache
        size_t hot_key_cache_size_mb
        uint32_t hot_key_promotion_threshold
        bool enable_negative_cache
        size_t negative_cache_entries
        bool enable_sorted_blocks
        bool enable_block_bloom_filters
        size_t max_background_threads

    cdef cppclass WriteOptions:
        WriteOptions()
        bool sync

    cdef cppclass ReadOptions:
        ReadOptions()
        bool verify_checksums
        bool fill_cache
        bool reverse_order

    cdef cppclass MarbleDB:
        @staticmethod
        Status Open(const DBOptions& options,
                   shared_ptr[Schema] schema,
                   unique_ptr[MarbleDB]* db)

        Status Put(const WriteOptions& options,
                  shared_ptr[Record] record)

        Status Get(const ReadOptions& options,
                  const Key& key,
                  shared_ptr[Record]* record)

        Status Delete(const WriteOptions& options,
                     const Key& key)

        Status Merge(const WriteOptions& options,
                    const Key& key,
                    const string& value)

        Status WriteBatch(const WriteOptions& options,
                         const vector[shared_ptr[Record]]& records)

        Status InsertBatch(const string& table_name,
                          const shared_ptr[CRecordBatch]& batch)

        Status ScanTable(const string& table_name,
                        unique_ptr[QueryResult]* result)

        Status CreateColumnFamily(const ColumnFamilyDescriptor& descriptor,
                                 ColumnFamilyHandle** handle)

        Status DropColumnFamily(ColumnFamilyHandle* handle)

        Status Flush()
        Status Close()
