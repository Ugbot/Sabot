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

namespace marble {

// Forward declarations
class Stream;
class ColumnFamilyHandle;
struct TransactionOptions;
class DBTransaction;
class SSTable;

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

    // Table operations
    virtual Status CreateTable(const TableSchema& schema) = 0;
    virtual Status ScanTable(const std::string& table_name,
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

    // Database management
    virtual Status Flush() = 0;
    virtual Status CompactRange(const KeyRange& range) = 0;
    virtual Status Destroy() = 0;

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
