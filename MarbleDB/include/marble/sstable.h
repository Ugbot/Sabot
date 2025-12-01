#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <marble/status.h>
#include <marble/file_system.h>

namespace marble {

// Forward declaration
class WideTableSchema;

// Tombstone marker - a special value that indicates a key was deleted
// Stored as 8 bytes to be distinguishable from empty values
static const std::string kTombstoneMarker = "\x7F\xDE\xAD\xBE\xEF\x00\x00\x01";

// ColumnStatistics needs full definition for std::vector in SSTableMetadata
// Using inline definition here to avoid circular includes with sstable_arrow.h
struct ColumnStatistics;  // Forward declare for now - use pointers in metadata

/**
 * @brief SSTable metadata for tracking file information
 */
struct SSTableMetadata {
    std::string filename;
    uint64_t file_size = 0;
    uint64_t min_key = 0;  // LSM storage key range (EncodeBatchKey for RecordBatch API)
    uint64_t max_key = 0;  // LSM storage key range (EncodeBatchKey for RecordBatch API)
    uint64_t record_count = 0;
    uint64_t created_timestamp = 0;
    uint64_t level = 0;  // LSM level (0 = L0, 1 = L1, etc.)
    std::string bloom_filter;  // Serialized bloom filter data

    // Data column value ranges (for RecordBatch predicate pushdown)
    // These track min/max values from the first data column, enabling
    // SSTable-level zone map pruning for RecordBatch queries
    uint64_t data_min_key = 0;   // Min value from first data column
    uint64_t data_max_key = 0;   // Max value from first data column
    bool has_data_range = false; // True if data_min_key/data_max_key are valid

    // Per-column statistics for full zone map support (wide-table API)
    // Using shared_ptr to avoid requiring full ColumnStatistics definition here
    std::vector<std::shared_ptr<ColumnStatistics>> column_stats;

    // Arrow schema stored in the SSTable (for wide-table API)
    std::shared_ptr<arrow::Schema> arrow_schema;

    // Serialization
    Status SerializeToString(std::string* output) const;
    static Status DeserializeFromString(const std::string& input, SSTableMetadata* metadata);

    /**
     * @brief Check if this SSTable may contain rows matching a predicate
     *
     * Uses zone maps (column_stats) to determine if predicate can possibly
     * match any data in this SSTable.
     *
     * @param predicate Arrow compute expression
     * @return true if SSTable may contain matches, false if definitely not
     */
    bool MayContainMatches(const arrow::compute::Expression& predicate) const;
};

/**
 * @brief Index entry for SSTable key lookup
 */
struct SSTableIndexEntry {
    uint64_t key;
    uint64_t offset;  // Byte offset in the SSTable file
    uint32_t size;    // Size of the value in bytes

    SSTableIndexEntry() : key(0), offset(0), size(0) {}
    SSTableIndexEntry(uint64_t k, uint64_t off, uint32_t sz)
        : key(k), offset(off), size(sz) {}
};

/**
 * @brief SSTable (Sorted String Table) - immutable, sorted key-value storage
 *
 * SSTables are the fundamental storage unit in an LSM tree. They contain:
 * - Sorted key-value pairs
 * - Index for fast key lookup
 * - Bloom filter for efficient key existence checks
 * - Metadata for compaction and statistics
 */
class SSTableImpl;  // Forward declaration for internal use

class SSTable {
public:
    virtual ~SSTable() = default;

    /**
     * @brief Get the SSTable metadata
     */
    virtual const SSTableMetadata& GetMetadata() const = 0;

    /**
     * @brief Check if a key exists in this SSTable
     */
    virtual bool ContainsKey(uint64_t key) const = 0;

    /**
     * @brief Get a value by key
     */
    virtual Status Get(uint64_t key, std::string* value) const = 0;

    /**
     * @brief Get multiple values by keys (batch operation)
     */
    virtual Status MultiGet(const std::vector<uint64_t>& keys,
                           std::vector<std::string>* values) const = 0;

    /**
     * @brief Scan a range of keys
     */
    virtual Status Scan(uint64_t start_key, uint64_t end_key,
                       std::vector<std::pair<uint64_t, std::string>>* results) const = 0;

    /**
     * @brief Scan a range of keys and return Arrow RecordBatches (optimized)
     *
     * This method provides batch-level iteration instead of row-by-row,
     * enabling 10-100x faster scans by avoiding individual I/O per row.
     *
     * @param start_key Minimum key (inclusive)
     * @param end_key Maximum key (inclusive)
     * @param batches Output vector of RecordBatches containing [key, value] columns
     * @return Status indicating success or failure
     */
    virtual Status ScanBatches(uint64_t start_key, uint64_t end_key,
                              std::vector<std::shared_ptr<::arrow::RecordBatch>>* batches) const = 0;

    /**
     * @brief Scan with column projection (wide-table API)
     *
     * Reads only the requested columns from disk using Arrow IPC's native
     * column selection. This avoids I/O for columns not needed by the query.
     *
     * @param start_key Minimum key (inclusive)
     * @param end_key Maximum key (inclusive)
     * @param column_indices Column indices to read (empty = all columns)
     * @param batches Output vector of RecordBatches with projected columns
     * @return Status indicating success or failure
     */
    virtual Status ScanBatchesWithProjection(
        uint64_t start_key, uint64_t end_key,
        const std::vector<int>& column_indices,
        std::vector<std::shared_ptr<::arrow::RecordBatch>>* batches) const {
        // Default implementation: fall back to full scan, then project
        // Subclasses should override for true column projection at I/O level
        return ScanBatches(start_key, end_key, batches);
    }

    /**
     * @brief Scan with predicate pushdown and projection (wide-table API)
     *
     * Combines zone map pruning, column projection, and predicate filtering
     * for maximum query efficiency.
     *
     * @param start_key Minimum key (inclusive)
     * @param end_key Maximum key (inclusive)
     * @param column_indices Column indices to read (empty = all columns)
     * @param filter Arrow compute expression for row filtering
     * @param batches Output vector of filtered RecordBatches
     * @return Status indicating success or failure
     */
    virtual Status ScanBatchesWithFilter(
        uint64_t start_key, uint64_t end_key,
        const std::vector<int>& column_indices,
        const std::shared_ptr<arrow::compute::Expression>& filter,
        std::vector<std::shared_ptr<::arrow::RecordBatch>>* batches) const {
        // Default implementation: scan with projection, then apply filter
        RETURN_NOT_OK(ScanBatchesWithProjection(start_key, end_key, column_indices, batches));

        if (filter && batches && !batches->empty()) {
            // Apply filter to each batch using Arrow Compute
            std::vector<std::shared_ptr<::arrow::RecordBatch>> filtered;
            for (const auto& batch : *batches) {
                // Evaluate expression to get boolean mask
                auto exec_ctx = arrow::compute::ExecContext();
                auto expr_result = arrow::compute::ExecuteScalarExpression(
                    *filter, *batch->schema(), arrow::Datum(batch), &exec_ctx);
                if (!expr_result.ok()) continue;

                auto mask = expr_result.ValueOrDie();
                if (!mask.is_array()) continue;

                // Filter the batch using the boolean mask
                auto filter_result = arrow::compute::Filter(batch, mask.make_array());
                if (filter_result.ok()) {
                    auto filtered_datum = filter_result.ValueOrDie();
                    if (filtered_datum.kind() == arrow::Datum::RECORD_BATCH) {
                        auto filtered_batch = filtered_datum.record_batch();
                        if (filtered_batch->num_rows() > 0) {
                            filtered.push_back(filtered_batch);
                        }
                    }
                }
            }
            *batches = std::move(filtered);
        }

        return Status::OK();
    }

    /**
     * @brief Get all keys in this SSTable
     */
    virtual Status GetAllKeys(std::vector<uint64_t>* keys) const = 0;

    /**
     * @brief Get file path of this SSTable
     */
    virtual std::string GetFilePath() const = 0;

    /**
     * @brief Get file size in bytes
     */
    virtual uint64_t GetFileSize() const = 0;

    /**
     * @brief Check if SSTable is valid and readable
     */
    virtual Status Validate() const = 0;
};

/**
 * @brief SSTable Writer - creates new SSTables from sorted key-value data
 */
class SSTableWriter {
public:
    virtual ~SSTableWriter() = default;

    /**
     * @brief Add a key-value pair to the SSTable
     * Keys must be added in sorted order
     */
    virtual Status Add(uint64_t key, const std::string& value) = 0;

    /**
     * @brief Add a key-value pair with metadata to the SSTable
     *
     * This extended version stores timestamp and tombstone as proper Arrow columns
     * for efficient predicate pushdown and filtering.
     *
     * @param key Entry key (uint64_t)
     * @param value Entry value (string)
     * @param timestamp MVCC timestamp for ordering and visibility
     * @param is_tombstone True if this is a delete marker
     * @return Status OK on success
     */
    virtual Status AddWithMetadata(uint64_t key, const std::string& value,
                                   uint64_t timestamp, bool is_tombstone) {
        // Default implementation falls back to basic Add
        // Subclasses can override for full metadata support
        return Add(key, value);
    }

    /**
     * @brief Finalize the SSTable and write it to disk
     */
    virtual Status Finish(std::unique_ptr<SSTable>* sstable) = 0;

    /**
     * @brief Get current number of entries
     */
    virtual size_t GetEntryCount() const = 0;

    /**
     * @brief Get estimated file size
     */
    virtual size_t GetEstimatedSize() const = 0;
};

/**
 * @brief SSTable Reader - reads existing SSTables from disk
 */
class SSTableReader {
public:
    virtual ~SSTableReader() = default;

    /**
     * @brief Open an existing SSTable file
     */
    virtual Status Open(const std::string& filepath,
                       std::unique_ptr<SSTable>* sstable) = 0;

    /**
     * @brief Create SSTable from existing file (assumes valid format)
     */
    virtual Status CreateFromFile(const std::string& filepath,
                                 const SSTableMetadata& metadata,
                                 std::unique_ptr<SSTable>* sstable) = 0;
};

/**
 * @brief SSTable Manager - factory and utility functions
 */
class SSTableManager {
public:
    virtual ~SSTableManager() = default;

    /**
     * @brief Create a new SSTable writer
     */
    virtual Status CreateWriter(const std::string& filepath,
                               uint64_t level,
                               std::unique_ptr<SSTableWriter>* writer) = 0;

    /**
     * @brief Open an existing SSTable
     */
    virtual Status OpenSSTable(const std::string& filepath,
                              std::unique_ptr<SSTable>* sstable) = 0;

    /**
     * @brief List all SSTable files in a directory
     */
    virtual Status ListSSTables(const std::string& directory,
                               std::vector<std::string>* files) = 0;

    /**
     * @brief Delete an SSTable file
     */
    virtual Status DeleteSSTable(const std::string& filepath) = 0;

    /**
     * @brief Repair corrupted SSTable files
     */
    virtual Status RepairSSTable(const std::string& filepath) = 0;
};

// Factory functions
std::unique_ptr<SSTableManager> CreateSSTableManager(
    std::shared_ptr<FileSystem> fs = nullptr);

} // namespace marble

