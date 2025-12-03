# marbledb.pxd - C++ declarations for MarbleDB
# cython: language_level=3

from libc.stdint cimport int64_t, uint64_t, uint32_t
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
        int Compare(const Key& other) const

    cdef cppclass Int64Key(Key):
        Int64Key(int64_t value)

    cdef cppclass TripleKey(Key):
        TripleKey(int64_t subject, int64_t predicate, int64_t object)
        int64_t subject() const
        int64_t predicate() const
        int64_t object() const

    cdef cppclass Record:
        pass

    cdef cppclass KeyRange:
        KeyRange(shared_ptr[Key] start, bool start_inclusive,
                 shared_ptr[Key] end, bool end_inclusive)
        @staticmethod
        KeyRange All()
        @staticmethod
        KeyRange StartAt(shared_ptr[Key] start, bool inclusive)
        @staticmethod
        KeyRange EndAt(shared_ptr[Key] end, bool inclusive)

    cdef cppclass Iterator:
        bool Valid() const
        void Next()
        shared_ptr[Key] key() const
        shared_ptr[Record] value() const
        Status status() const
        void Seek(const Key& target)
        void SeekForPrev(const Key& target)
        void SeekToLast()
        void Prev()

cdef extern from "marble/query.h" namespace "marble":
    cdef cppclass QueryResult:
        bool HasNext() const
        Status Next(shared_ptr[CRecordBatch]* batch)
        shared_ptr[CSchema] schema() const
        int64_t num_rows() const
        int64_t num_batches() const

cdef extern from "marble/table.h" namespace "marble":
    cdef cppclass TableSchema:
        string table_name
        shared_ptr[CSchema] arrow_schema
        TableSchema()
        TableSchema(const string& name, shared_ptr[CSchema] schema)

cdef extern from "marble/table_capabilities.h" namespace "marble::TableCapabilities":
    # Temporal model enum (nested in TableCapabilities)
    cdef enum TemporalModel:
        kNone "marble::TableCapabilities::TemporalModel::kNone"
        kSystemTime "marble::TableCapabilities::TemporalModel::kSystemTime"
        kValidTime "marble::TableCapabilities::TemporalModel::kValidTime"
        kBitemporal "marble::TableCapabilities::TemporalModel::kBitemporal"

cdef extern from "marble/table_capabilities.h" namespace "marble::TableCapabilities::MVCCSettings":
    # GC Policy enum (nested in MVCCSettings)
    cdef enum GCPolicy:
        kKeepAllVersions "marble::TableCapabilities::MVCCSettings::GCPolicy::kKeepAllVersions"
        kKeepRecentVersions "marble::TableCapabilities::MVCCSettings::GCPolicy::kKeepRecentVersions"
        kKeepVersionsUntil "marble::TableCapabilities::MVCCSettings::GCPolicy::kKeepVersionsUntil"

cdef extern from "marble/table_capabilities.h" namespace "marble::TableCapabilities":
    # MVCCSettings struct
    cdef cppclass MVCCSettings:
        GCPolicy gc_policy
        size_t max_versions_per_key
        uint64_t gc_timestamp_ms

cdef extern from "marble/table_capabilities.h" namespace "marble":
    cdef cppclass TableCapabilities:
        TableCapabilities()
        TemporalModel temporal_model
        bool enable_mvcc
        MVCCSettings mvcc_settings

cdef extern from "marble/column_family.h" namespace "marble":
    cdef cppclass ColumnFamilyHandle:
        pass

    cdef cppclass ColumnFamilyOptions:
        ColumnFamilyOptions()
        shared_ptr[CSchema] schema
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

        # Iterator and compaction
        Status NewIterator(const ReadOptions& options,
                          const KeyRange& range,
                          unique_ptr[Iterator]* iterator)

        Status NewIterator(const string& table_name,
                          const ReadOptions& options,
                          const KeyRange& range,
                          unique_ptr[Iterator]* iterator)

        Status CompactRange(const KeyRange& range)

        # Temporal/Bitemporal operations
        Status TemporalScanDedup(const string& table_name,
                                 const vector[string]& key_columns,
                                 uint64_t query_time,
                                 uint64_t valid_time_start,
                                 uint64_t valid_time_end,
                                 bool include_deleted,
                                 unique_ptr[QueryResult]* result)

        Status TemporalUpdate(const string& table_name,
                             const vector[string]& key_columns,
                             const shared_ptr[CRecordBatch]& key_values,
                             const shared_ptr[CRecordBatch]& updated_batch)

        Status TemporalDelete(const string& table_name,
                             const vector[string]& key_columns,
                             const shared_ptr[CRecordBatch]& key_values)

        Status PruneVersions(const string& table_name,
                            uint64_t* versions_removed)

        Status PruneVersions(const string& table_name,
                            size_t max_versions_per_key,
                            uint64_t min_system_time_us,
                            uint64_t* versions_removed)

        Status CreateTable(const TableSchema& table_schema,
                          const TableCapabilities& capabilities)

cdef extern from "marble/api.h" namespace "marble":
    Status OpenDatabase(const string& db_path, unique_ptr[MarbleDB]* db)
    void CloseDatabase(unique_ptr[MarbleDB]* db)

# Join types and operations
cdef extern from "marble/advanced_query.h" namespace "marble":
    cdef enum JoinType:
        kInner "marble::JoinType::kInner"
        kLeft "marble::JoinType::kLeft"
        kRight "marble::JoinType::kRight"
        kFull "marble::JoinType::kFull"
        kCross "marble::JoinType::kCross"

    cdef cppclass AsofJoinSpec:
        string time_column
        vector[string] by_columns
        int64_t tolerance
        string left_suffix
        string right_suffix
        AsofJoinSpec()

    # Hash join - full signature
    Status HashJoin(
        shared_ptr[CTable] left,
        shared_ptr[CTable] right,
        const vector[string]& left_keys,
        const vector[string]& right_keys,
        JoinType join_type,
        const string& left_suffix,
        const string& right_suffix,
        shared_ptr[CTable]* result)

    # Hash join - simplified signature (same keys in both tables)
    Status HashJoin(
        shared_ptr[CTable] left,
        shared_ptr[CTable] right,
        const vector[string]& on_keys,
        JoinType join_type,
        shared_ptr[CTable]* result)

    # ASOF join for time series data
    Status AsofJoin(
        shared_ptr[CTable] left,
        shared_ptr[CTable] right,
        const AsofJoinSpec& spec,
        shared_ptr[CTable]* result)
