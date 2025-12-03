#pragma once

#include <memory>
#include <string>
#include <vector>
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/record.h>
#include <marble/table.h>
#include <marble/column_family.h>
#include <marble/merge_operator.h>
#include <marble/record_ref.h>
#include <marble/metrics.h>
#include <marble/table_capabilities.h>

namespace marble {

// Forward declarations
class Stream;
class ColumnFamilyHandle;
struct TransactionOptions;
class DBTransaction;
class SSTable;
class TableBatchIterator;

/**
 * @brief Streaming iterator for Arrow RecordBatches from a MarbleDB table
 *
 * Provides lazy iteration over RecordBatches from MarbleDB tables.
 * This is the core interface for zero-copy streaming access:
 * - No materialization: batches are fetched on-demand
 * - Memory efficient: only one batch in memory at a time
 * - Predicate pushdown: zone maps prune non-matching SSTables
 *
 * Note: This is distinct from RecordBatchIterator (in record_ref.h) which
 * iterates over rows within a single batch. This class iterates over
 * multiple batches from a table scan.
 *
 * Usage:
 *   std::unique_ptr<TableBatchIterator> iter;
 *   db->NewBatchIterator("table", &iter);
 *   while (iter->Valid()) {
 *       std::shared_ptr<arrow::RecordBatch> batch;
 *       iter->GetBatch(&batch);
 *       // Process batch...
 *       iter->Next();
 *   }
 */
class TableBatchIterator {
public:
    virtual ~TableBatchIterator() = default;

    /**
     * @brief Check if iterator has more batches
     * @return true if current position is valid
     */
    virtual bool Valid() const = 0;

    /**
     * @brief Move to the next batch
     */
    virtual void Next() = 0;

    /**
     * @brief Get the current batch (zero-copy)
     * @param batch Output parameter for the current batch
     * @return Status OK on success, InvalidArgument if not valid
     */
    virtual Status GetBatch(std::shared_ptr<::arrow::RecordBatch>* batch) const = 0;

    /**
     * @brief Get the schema for batches in this iterator
     * @return Arrow schema
     */
    virtual std::shared_ptr<::arrow::Schema> schema() const = 0;

    /**
     * @brief Get the current status of the iterator
     * @return Status OK if no errors
     */
    virtual Status status() const = 0;

    /**
     * @brief Get approximate number of remaining batches
     * @return Estimated batch count (may be inexact)
     */
    virtual int64_t GetApproximateRemainingBatches() const = 0;
};

// Forward declaration for arrow_api friend
namespace arrow_api {
    class MarbleRecordBatchReader;

    ::arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> OpenTable(
        std::shared_ptr<MarbleDB> db,
        const std::string& table_name,
        const std::vector<std::string>& projection,
        const std::vector<ColumnPredicate>& predicates);
}

// Database configuration options
struct DBOptions {
    // Database path
    std::string db_path = "/tmp/marble";

    // Memtable size threshold (bytes)
    size_t memtable_size_threshold = 64 * 1024 * 1024; // 64MB

    // SSTable size threshold (bytes)
    size_t sstable_size_threshold = 256 * 1024 * 1024; // 256MB

    // Block size for SSTable blocks (bytes)
    size_t block_size = 64 * 1024; // 64KB

    // Compression type for SSTables
    enum class CompressionType {
        kNoCompression,
        kSnappy,
        kLZ4,
        kZSTD
    };
    CompressionType compression = CompressionType::kLZ4;

    // WAL options
    bool enable_wal = true;
    size_t wal_buffer_size = 64 * 1024 * 1024; // 64MB

    // Compaction options
    size_t max_level0_files = 4;
    size_t max_level_files_base = 8;
    size_t level_multiplier = 2;

    // ClickHouse-style indexing options
    bool enable_sparse_index = true;
    size_t index_granularity = 8192;  // Index every N rows (sparse index)
    size_t target_block_size = 8192;  // Target rows per block
    bool enable_bloom_filter = true;
    size_t bloom_filter_bits_per_key = 10;  // Bloom filter size
    
    // Point lookup optimizations
    bool enable_hot_key_cache = true;
    size_t hot_key_cache_size_mb = 64;
    uint32_t hot_key_promotion_threshold = 3;
    bool enable_negative_cache = true;
    size_t negative_cache_entries = 10000;
    bool enable_sorted_blocks = true;  // Sort keys within blocks for binary search
    bool enable_block_bloom_filters = true;  // Bloom filter per block

    // Threading options
    size_t max_background_threads = 4;
};

// Write options
struct WriteOptions {
    // Sync WAL after each write
    bool sync = false;
};

// Read options
struct ReadOptions {
    // Verify checksums
    bool verify_checksums = false;

    // Fill cache
    bool fill_cache = true;

    // Scan in reverse order (descending)
    bool reverse_order = false;
};

// Main MarbleDB class
class MarbleDB {
    // Friend declarations for Arrow integration
    friend ::arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>>
        arrow_api::OpenTable(std::shared_ptr<MarbleDB>, const std::string&,
                            const std::vector<std::string>&, const std::vector<ColumnPredicate>&);
    friend class arrow_api::MarbleRecordBatchReader;

public:
    MarbleDB() = default;
    virtual ~MarbleDB() = default;

    // Factory method to open/create a database
    static Status Open(const DBOptions& options,
                       std::shared_ptr<Schema> schema,
                       std::unique_ptr<MarbleDB>* db);

    // Basic operations
    virtual Status Put(const WriteOptions& options,
                       std::shared_ptr<Record> record) = 0;

    virtual Status Get(const ReadOptions& options,
                       const Key& key,
                       std::shared_ptr<Record>* record) = 0;

    virtual Status Delete(const WriteOptions& options,
                          const Key& key) = 0;
    
    /**
     * @brief Merge operation for associative updates
     * 
     * Applies merge operator to combine new value with existing value.
     * More efficient than Get + Modify + Put for counters, sets, etc.
     * 
     * @param options Write options
     * @param key Key to merge
     * @param value Merge operand
     * @return Status OK on success
     */
    virtual Status Merge(const WriteOptions& options,
                        const Key& key,
                        const std::string& value) = 0;
    
    // Column family operations
    virtual Status Merge(const WriteOptions& options,
                        ColumnFamilyHandle* cf,
                        const Key& key,
                        const std::string& value) = 0;

    // Batch operations
    virtual Status WriteBatch(const WriteOptions& options,
                              const std::vector<std::shared_ptr<Record>>& records) = 0;

    // Arrow batch operations
    virtual Status InsertBatch(const std::string& table_name,
                              const std::shared_ptr<arrow::RecordBatch>& batch) = 0;

    /**
     * @brief Temporal update for bitemporal/system-time tables
     *
     * For temporal tables, updates don't overwrite data. Instead:
     * 1. Close existing versions (set _system_time_end = now)
     * 2. Insert new versions with updated values (_system_time_start = now)
     *
     * This maintains full history for time-travel queries.
     *
     * @param table_name Name of the temporal table
     * @param key_columns Names of columns that identify rows to update
     * @param key_values Values of key columns to match
     * @param updated_batch New data for matched rows (only user columns)
     * @return Status OK on success, InvalidArgument if table not temporal
     */
    virtual Status TemporalUpdate(const std::string& table_name,
                                 const std::vector<std::string>& key_columns,
                                 const std::shared_ptr<arrow::RecordBatch>& key_values,
                                 const std::shared_ptr<arrow::RecordBatch>& updated_batch) = 0;

    /**
     * @brief Temporal delete for bitemporal/system-time tables (soft delete)
     *
     * For temporal tables, deletes don't remove data. Instead:
     * 1. Close existing versions (set _system_time_end = now)
     * 2. Insert tombstone versions with _is_deleted = true
     *
     * This maintains full history and allows time-travel queries
     * to see data that existed at past points in time.
     *
     * @param table_name Name of the temporal table
     * @param key_columns Names of columns that identify rows to delete
     * @param key_values Values of key columns to match
     * @return Status OK on success, InvalidArgument if table not temporal
     */
    virtual Status TemporalDelete(const std::string& table_name,
                                 const std::vector<std::string>& key_columns,
                                 const std::shared_ptr<arrow::RecordBatch>& key_values) = 0;

    // Table operations
    virtual Status CreateTable(const TableSchema& schema) = 0;

    /**
     * @brief Create a table with specific capabilities
     *
     * Creates a table with the given schema and capabilities. For bitemporal
     * or system-time tables, the schema is automatically augmented with
     * temporal columns:
     * - _system_time_start: When the version was created
     * - _system_time_end: When the version was superseded
     * - _valid_time_start: When the data is/was valid (bitemporal only)
     * - _valid_time_end: When the data validity ends (bitemporal only)
     * - _is_deleted: Soft delete flag for audit trail
     *
     * @param schema Table schema (will be augmented for temporal tables)
     * @param caps Table capabilities including temporal model
     * @return Status OK on success
     */
    virtual Status CreateTable(const TableSchema& schema,
                              const TableCapabilities& caps) = 0;

    virtual Status ScanTable(const std::string& table_name,
                            std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Temporal scan with time-travel support
     *
     * Scans a bitemporal table with system time and/or valid time constraints.
     * Uses zone map pruning to skip batches that don't match temporal criteria.
     *
     * @param table_name Name of the table to scan
     * @param system_time_at Point-in-time system time query (AS OF), 0 for current
     * @param valid_time_start Valid time range start (0 for unbounded)
     * @param valid_time_end Valid time range end (MAX for unbounded)
     * @param include_deleted Whether to include soft-deleted records
     * @param result Output query result
     * @return Status
     */
    virtual Status TemporalScan(const std::string& table_name,
                               uint64_t system_time_at,
                               uint64_t valid_time_start,
                               uint64_t valid_time_end,
                               bool include_deleted,
                               std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Temporal scan with version deduplication
     *
     * Like TemporalScan but also deduplicates by business key, keeping only
     * the latest version (highest _system_time_start) for each unique key.
     * This provides "latest wins" semantics for append-only storage.
     *
     * @param table_name Name of the table to scan
     * @param key_columns Business key column names for deduplication
     * @param system_time_at Point-in-time system time query (AS OF), 0 for current
     * @param valid_time_start Valid time range start (0 for unbounded)
     * @param valid_time_end Valid time range end (MAX for unbounded)
     * @param include_deleted Whether to include soft-deleted records
     * @param result Output query result
     * @return Status
     */
    virtual Status TemporalScanDedup(const std::string& table_name,
                                    const std::vector<std::string>& key_columns,
                                    uint64_t system_time_at,
                                    uint64_t valid_time_start,
                                    uint64_t valid_time_end,
                                    bool include_deleted,
                                    std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Column Family operations
     */
    virtual Status CreateColumnFamily(const ColumnFamilyDescriptor& descriptor,
                                     ColumnFamilyHandle** handle) = 0;
    virtual Status DropColumnFamily(ColumnFamilyHandle* handle) = 0;
    virtual std::vector<std::string> ListColumnFamilies() const = 0;
    
    // CF-specific operations
    virtual Status Put(const WriteOptions& options,
                      ColumnFamilyHandle* cf,
                      std::shared_ptr<Record> record) = 0;
    
    virtual Status Get(const ReadOptions& options,
                      ColumnFamilyHandle* cf,
                      const Key& key,
                      std::shared_ptr<Record>* record) = 0;
    
    virtual Status Delete(const WriteOptions& options,
                         ColumnFamilyHandle* cf,
                         const Key& key) = 0;
    
    /**
     * @brief Multi-Get for batch point lookups
     * 
     * Faster than individual Get() calls due to:
     * - Single lock acquisition
     * - Batch I/O operations
     * - Cache-friendly access patterns
     * 
     * @return Status OK if all keys processed (even if some not found)
     */
    virtual Status MultiGet(const ReadOptions& options,
                           const std::vector<Key>& keys,
                           std::vector<std::shared_ptr<Record>>* records) = 0;
    
    virtual Status MultiGet(const ReadOptions& options,
                           ColumnFamilyHandle* cf,
                           const std::vector<Key>& keys,
                           std::vector<std::shared_ptr<Record>>* records) = 0;
    
    /**
     * @brief Delete range of keys efficiently
     * 
     * Much faster than loop of Delete() calls.
     * Uses tombstones for efficient bulk deletion.
     */
    virtual Status DeleteRange(const WriteOptions& options,
                              const Key& begin_key,
                              const Key& end_key) = 0;
    
    virtual Status DeleteRange(const WriteOptions& options,
                              ColumnFamilyHandle* cf,
                              const Key& begin_key,
                              const Key& end_key) = 0;

    // Scanning
    virtual Status NewIterator(const ReadOptions& options,
                               const KeyRange& range,
                               std::unique_ptr<Iterator>* iterator) = 0;

    // Scanning with column family
    virtual Status NewIterator(const std::string& table_name,
                               const ReadOptions& options,
                               const KeyRange& range,
                               std::unique_ptr<Iterator>* iterator) = 0;

    /**
     * @brief Create a streaming batch iterator for a table
     *
     * Returns a lazy iterator that yields RecordBatches on demand.
     * This is the preferred API for streaming access as it:
     * - Avoids materializing all results in memory
     * - Enables pipelined processing
     * - Supports predicate pushdown via zone maps
     *
     * @param table_name Name of the table to scan
     * @param iter Output iterator
     * @return Status OK on success
     */
    virtual Status NewBatchIterator(const std::string& table_name,
                                    std::unique_ptr<TableBatchIterator>* iter) = 0;

    /**
     * @brief Create a streaming batch iterator with predicate pushdown
     *
     * Like NewBatchIterator but with column predicates for filtering.
     * Zone maps are used to skip SSTables that don't match predicates.
     *
     * @param table_name Name of the table to scan
     * @param predicates Column predicates for filtering
     * @param iter Output iterator
     * @return Status OK on success
     */
    virtual Status NewBatchIterator(const std::string& table_name,
                                    const std::vector<ColumnPredicate>& predicates,
                                    std::unique_ptr<TableBatchIterator>* iter) = 0;

    /**
     * @brief Fast batch-based range scan (10-100x faster than Iterator)
     *
     * Returns RecordBatches directly from LSM tree for optimal throughput.
     * Uses batch-level zone map pruning for efficient data skipping.
     *
     * @param start_key Start of range (0 for beginning)
     * @param end_key End of range (UINT64_MAX for end)
     * @param batches Output vector of RecordBatches
     * @return Status OK on success
     */
    virtual Status ScanBatches(uint64_t start_key, uint64_t end_key,
                              std::vector<std::shared_ptr<::arrow::RecordBatch>>* batches) = 0;

    /**
     * @brief Scan with predicate pushdown (100-1000x faster)
     *
     * Enables short-circuiting disk reads using zone maps, bloom filters, and SIMD.
     * Optimization strategies check predicates against statistics to skip SSTables.
     *
     * Expected performance improvements:
     * - WHERE column = 'value': 100-200x faster (bloom filter + short-circuit)
     * - WHERE column > threshold: 100-1000x faster (zone map pruning)
     * - WHERE column LIKE '%pattern%': 30-40x faster (SIMD + short-circuit)
     *
     * @param start_key Start of range (inclusive)
     * @param end_key End of range (inclusive)
     * @param predicates Column predicates for filtering
     * @param batches Output vector of RecordBatches
     * @return Status OK on success
     */
    virtual Status ScanBatchesWithPredicates(uint64_t start_key, uint64_t end_key,
                                             const std::vector<ColumnPredicate>& predicates,
                                             std::vector<std::shared_ptr<::arrow::RecordBatch>>* batches) = 0;

    // Database management
    virtual Status Flush() = 0;
    virtual Status CompactRange(const KeyRange& range) = 0;
    virtual Status Destroy() = 0;

    // Version garbage collection for temporal tables
    /**
     * @brief Prune old versions from a temporal table
     *
     * For bitemporal tables, this removes old versions based on the table's
     * MVCCSettings GC policy:
     * - kKeepAllVersions: No pruning (for audit)
     * - kKeepRecentVersions: Keep N most recent versions per key
     * - kKeepVersionsUntil: Remove versions with sys_end < timestamp
     *
     * @param table_name The table to prune
     * @param versions_removed Output: number of versions removed
     * @return Status OK if successful
     */
    virtual Status PruneVersions(const std::string& table_name,
                                 uint64_t* versions_removed) = 0;

    /**
     * @brief Prune old versions with custom parameters
     *
     * Allows overriding the table's default GC settings.
     *
     * @param table_name The table to prune
     * @param max_versions_per_key Maximum versions to keep per business key
     * @param min_system_time_us Prune versions with sys_end < this timestamp
     * @param versions_removed Output: number of versions removed
     * @return Status OK if successful
     */
    virtual Status PruneVersions(const std::string& table_name,
                                 size_t max_versions_per_key,
                                 uint64_t min_system_time_us,
                                 uint64_t* versions_removed) = 0;

    // Checkpointing and state management
    virtual Status CreateCheckpoint(const std::string& checkpoint_path) = 0;
    virtual Status RestoreFromCheckpoint(const std::string& checkpoint_path) = 0;
    virtual Status GetCheckpointMetadata(std::string* metadata) const = 0;

    // Streaming interfaces for operator/agent communication
    virtual Status CreateStream(const std::string& stream_name,
                               std::unique_ptr<Stream>* stream) = 0;
    virtual Status GetStream(const std::string& stream_name,
                           std::unique_ptr<Stream>* stream) = 0;

    // Statistics and info
    virtual std::string GetProperty(const std::string& property) const = 0;
    virtual Status GetApproximateSizes(const std::vector<KeyRange>& ranges,
                                       std::vector<uint64_t>* sizes) const = 0;

    // Monitoring and metrics (production features)
    /**
     * @brief Get metrics collector for this database
     *
     * Returns the metrics collector used by this database instance.
     * Metrics include operation counts, latencies, cache statistics, etc.
     */
    virtual std::shared_ptr<MetricsCollector> GetMetricsCollector() const = 0;

    /**
     * @brief Export metrics in Prometheus format
     *
     * Returns metrics in Prometheus exposition format for monitoring systems.
     */
    virtual std::string ExportMetricsPrometheus() const = 0;

    /**
     * @brief Export metrics in JSON format
     *
     * Returns metrics in JSON format for custom monitoring or debugging.
     */
    virtual std::string ExportMetricsJSON() const = 0;

    /**
     * @brief Get health status of database components
     *
     * Returns health status of various database components like:
     * - Storage engine
     * - Cache systems
     * - Background compaction
     * - Network connectivity (if applicable)
     */
    virtual std::unordered_map<std::string, bool> GetHealthStatus() const = 0;

    /**
     * @brief Perform health checks and return detailed status
     *
     * Runs comprehensive health checks including:
     * - File system accessibility
     * - Data integrity verification
     * - Memory usage validation
     * - Background process health
     */
    virtual StatusWithMetrics PerformHealthCheck() const = 0;

    /**
     * @brief Get detailed system information
     *
     * Returns comprehensive system information including:
     * - Database version and build info
     * - Storage statistics (size, file count, etc.)
     * - Cache hit rates and performance
     * - Background operation status
     * - Memory usage breakdown
     */
    virtual std::string GetSystemInfo() const = 0;

    // Transaction support (MVCC with snapshot isolation)
    virtual Status BeginTransaction(const TransactionOptions& options,
                                  DBTransaction** txn) = 0;
    virtual Status BeginTransaction(DBTransaction** txn) = 0;  // Default options

protected:
    /**
     * @brief Create Arrow RecordBatchReader for a table (internal use by arrow_api)
     *
     * This method is used internally by arrow_api::OpenTable() factory function.
     * It creates a fully initialized RecordBatchReader with access to LSM internals.
     * This keeps the LSM structure hidden from the public API while enabling
     * efficient Arrow integration.
     *
     * @param table_name Name of the table
     * @param projection Column names to read (empty = all columns)
     * @param predicates Predicates for pushdown
     * @param reader Output RecordBatchReader
     * @return Status OK on success
     */
    virtual Status CreateRecordBatchReader(
        const std::string& table_name,
        const std::vector<std::string>& projection,
        const std::vector<ColumnPredicate>& predicates,
        std::shared_ptr<::arrow::RecordBatchReader>* reader) = 0;

    /**
     * @brief Get SSTables for a table (internal use - protected, not public API)
     *
     * Returns SSTables organized by LSM level. This is a protected method
     * used internally by CreateRecordBatchReader() to access LSM structure.
     * NOT part of the public API - use arrow_api::OpenTable() instead.
     *
     * @param table_name Name of the table
     * @param sstables Output vector of SSTables per level (index 0 = L0, 1 = L1, etc.)
     * @return Status OK on success
     */
    virtual Status GetSSTablesInternal(
        const std::string& table_name,
        std::vector<std::vector<std::shared_ptr<SSTable>>>* sstables) = 0;

    /**
     * @brief Get table schema (internal use - protected, not public API)
     *
     * Returns the Arrow schema for a table. This is a protected method
     * used internally by CreateRecordBatchReader() to determine table columns.
     * NOT part of the public API - use arrow_api::OpenTable() instead.
     *
     * @param table_name Name of the table
     * @param schema Output Arrow schema
     * @return Status OK on success, InvalidArgument if table doesn't exist
     */
    virtual Status GetTableSchemaInternal(
        const std::string& table_name,
        std::shared_ptr<::arrow::Schema>* schema) = 0;

    // Disable copying
    MarbleDB(const MarbleDB&) = delete;
    MarbleDB& operator=(const MarbleDB&) = delete;
};

// Forward declarations for MVCC
class MVCCManager;
class Snapshot;

// Transaction options
struct TransactionOptions {
    bool read_only = false;         // Read-only transaction
    bool snapshot = true;           // Use snapshot isolation
    uint64_t lock_timeout_ms = 5000; // Lock timeout in milliseconds
    size_t max_write_buffer_size = 64 * 1024 * 1024; // 64MB max buffer

    TransactionOptions() = default;
};

// Transaction support with MVCC
class DBTransaction {
public:
    virtual ~DBTransaction() = default;

    // Read operations (snapshot isolation)
    virtual Status Get(const ReadOptions& options, const Key& key,
                      std::shared_ptr<Record>* record) = 0;

    // Write operations (buffered until commit)
    virtual Status Put(const WriteOptions& options, std::shared_ptr<Record> record) = 0;
    virtual Status Delete(const WriteOptions& options, const Key& key) = 0;

    // Batch operations within transaction
    virtual Status Put(const WriteOptions& options, ColumnFamilyHandle* cf,
                      std::shared_ptr<Record> record) = 0;
    virtual Status Delete(const WriteOptions& options, ColumnFamilyHandle* cf,
                         const Key& key) = 0;

    // Transaction control
    virtual Status Commit() = 0;
    virtual Status Rollback() = 0;

    // Transaction metadata
    virtual uint64_t GetTxnId() const = 0;
    virtual Snapshot GetSnapshot() const = 0;
    virtual bool IsReadOnly() const = 0;
};

// Batch write support
class WriteBatch {
public:
    WriteBatch() = default;
    virtual ~WriteBatch() = default;

    virtual Status Put(std::shared_ptr<Record> record) = 0;
    virtual Status Delete(const Key& key) = 0;

    virtual void Clear() = 0;
    virtual size_t Count() const = 0;
};

// Utility functions
Status DestroyDB(const std::string& db_path, const DBOptions& options);
Status RepairDB(const std::string& db_path, const DBOptions& options);

} // namespace marble
