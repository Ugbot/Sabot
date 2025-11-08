#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/file_system.h>

namespace marble {

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

    // Serialization
    Status SerializeToString(std::string* output) const;
    static Status DeserializeFromString(const std::string& input, SSTableMetadata* metadata);
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

