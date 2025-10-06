#pragma once

#include <memory>
#include <string>
#include <marble/status.h>
#include <marble/record.h>

namespace marble {

// Forward declarations
class Stream;

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

    // Batch operations
    virtual Status WriteBatch(const WriteOptions& options,
                              const std::vector<std::shared_ptr<Record>>& records) = 0;

    // Scanning
    virtual Status NewIterator(const ReadOptions& options,
                               const KeyRange& range,
                               std::unique_ptr<Iterator>* iterator) = 0;

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

    // Disable copying
    MarbleDB(const MarbleDB&) = delete;
    MarbleDB& operator=(const MarbleDB&) = delete;
};

// Transaction support (future extension)
class DBTransaction {
public:
    virtual ~DBTransaction() = default;

    virtual Status Put(std::shared_ptr<Record> record) = 0;
    virtual Status Get(const Key& key, std::shared_ptr<Record>* record) = 0;
    virtual Status Delete(const Key& key) = 0;

    virtual Status Commit() = 0;
    virtual Status Rollback() = 0;
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
