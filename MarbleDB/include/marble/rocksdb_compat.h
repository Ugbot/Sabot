/**
 * RocksDB Compatibility Layer for MarbleDB
 *
 * Provides a drop-in RocksDB-compatible API over MarbleDB's LSM storage.
 * Enables easy migration from RocksDB to MarbleDB with minimal code changes.
 */

#pragma once

#include "marble/db.h"
#include "marble/status.h"
#include <string>
#include <string_view>
#include <vector>
#include <memory>

namespace marble {
namespace rocksdb {

// Type aliases for RocksDB compatibility
using Slice = std::string_view;

// Forward declarations
class DB;
class Iterator;
class WriteBatch;
class ColumnFamilyHandle;

//==============================================================================
// Options
//==============================================================================

struct Options {
    // Create if missing
    bool create_if_missing = true;

    // Error if exists
    bool error_if_exists = false;

    // Write buffer size (memtable size)
    size_t write_buffer_size = 64 * 1024 * 1024;  // 64MB

    // Max write buffer number
    int max_write_buffer_number = 2;

    // Level 0 file number compaction trigger
    int level0_file_num_compaction_trigger = 4;

    // Max bytes for level-1
    uint64_t max_bytes_for_level_base = 256 * 1024 * 1024;  // 256MB

    // Target file size
    uint64_t target_file_size_base = 64 * 1024 * 1024;  // 64MB

    // Compression type
    enum CompressionType {
        kNoCompression = 0,
        kSnappyCompression = 1,
        kZlibCompression = 2,
        kLZ4Compression = 4
    };
    CompressionType compression = kNoCompression;

    // Max open files
    int max_open_files = 1000;

    // Use fsync
    bool use_fsync = false;

    // Paranoid checks
    bool paranoid_checks = false;
};

struct ReadOptions {
    // Verify checksums
    bool verify_checksums = false;

    // Fill cache
    bool fill_cache = true;

    // Snapshot
    const void* snapshot = nullptr;

    // Read tier
    enum ReadTier {
        kReadAllTier = 0,
        kBlockCacheTier = 1
    };
    ReadTier read_tier = kReadAllTier;
};

struct WriteOptions {
    // Sync
    bool sync = false;

    // Disable WAL
    bool disableWAL = false;

    // Ignore missing column families
    bool ignore_missing_column_families = false;
};

struct FlushOptions {
    // Wait
    bool wait = true;

    // Allow write stall
    bool allow_write_stall = false;
};

struct CompactRangeOptions {
    // Exclusive manual compaction
    bool exclusive_manual_compaction = true;

    // Change level
    bool change_level = false;

    // Target level
    int target_level = -1;

    // Max subcompactions
    uint32_t max_subcompactions = 0;
};

struct ColumnFamilyOptions {
    // Write buffer size
    size_t write_buffer_size = 64 * 1024 * 1024;

    // Max write buffer number
    int max_write_buffer_number = 2;

    // Compression
    Options::CompressionType compression = Options::kNoCompression;
};

//==============================================================================
// WriteBatch - Batch multiple writes
//==============================================================================

class WriteBatch {
public:
    WriteBatch();
    ~WriteBatch();

    // Put a key-value pair
    void Put(const Slice& key, const Slice& value);
    void Put(ColumnFamilyHandle* cf, const Slice& key, const Slice& value);

    // Delete a key
    void Delete(const Slice& key);
    void Delete(ColumnFamilyHandle* cf, const Slice& key);

    // Merge a key-value pair
    void Merge(const Slice& key, const Slice& value);
    void Merge(ColumnFamilyHandle* cf, const Slice& key, const Slice& value);

    // Clear all updates
    void Clear();

    // Get the number of updates
    int Count() const;

private:
    friend class DB;

    struct Operation {
        enum Type { PUT, DELETE, MERGE };
        Type type;
        ColumnFamilyHandle* cf;
        std::string key;
        std::string value;
    };

    std::vector<Operation> operations_;
};

//==============================================================================
// Iterator - Range iteration
//==============================================================================

class Iterator {
public:
    virtual ~Iterator();

    // Check if valid
    virtual bool Valid() const = 0;

    // Seek to first
    virtual void SeekToFirst() = 0;

    // Seek to last
    virtual void SeekToLast() = 0;

    // Seek to key
    virtual void Seek(const Slice& target) = 0;

    // Seek for prev
    virtual void SeekForPrev(const Slice& target) = 0;

    // Move to next
    virtual void Next() = 0;

    // Move to prev
    virtual void Prev() = 0;

    // Get current key
    virtual Slice key() const = 0;

    // Get current value
    virtual Slice value() const = 0;

    // Get status
    virtual marble::Status status() const = 0;
};

//==============================================================================
// ColumnFamilyHandle - Handle to a column family
//==============================================================================

class ColumnFamilyHandle {
public:
    virtual ~ColumnFamilyHandle() = default;

    // Get column family name
    virtual const std::string& GetName() const = 0;

    // Get column family ID
    virtual uint32_t GetID() const = 0;

private:
    friend class DB;
    marble::ColumnFamilyHandle* impl_;
};

//==============================================================================
// DB - Main database interface
//==============================================================================

class DB {
public:
    virtual ~DB();

    //==========================================================================
    // Factory - Open database
    //==========================================================================

    static marble::Status Open(const Options& options, const std::string& name, DB** dbptr);

    //==========================================================================
    // Basic operations
    //==========================================================================

    // Put a key-value pair
    virtual marble::Status Put(const WriteOptions& options, const Slice& key, const Slice& value);
    virtual marble::Status Put(const WriteOptions& options, ColumnFamilyHandle* cf,
                               const Slice& key, const Slice& value);

    // Get a value by key
    virtual marble::Status Get(const ReadOptions& options, const Slice& key, std::string* value);
    virtual marble::Status Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                               const Slice& key, std::string* value);

    // Delete a key
    virtual marble::Status Delete(const WriteOptions& options, const Slice& key);
    virtual marble::Status Delete(const WriteOptions& options, ColumnFamilyHandle* cf,
                                  const Slice& key);

    // Merge a key-value pair
    virtual marble::Status Merge(const WriteOptions& options, const Slice& key, const Slice& value);
    virtual marble::Status Merge(const WriteOptions& options, ColumnFamilyHandle* cf,
                                 const Slice& key, const Slice& value);

    //==========================================================================
    // Batch operations
    //==========================================================================

    // Write a batch
    virtual marble::Status Write(const WriteOptions& options, WriteBatch* batch);

    // Multi-get
    virtual std::vector<marble::Status> MultiGet(const ReadOptions& options,
                                                  const std::vector<Slice>& keys,
                                                  std::vector<std::string>* values);

    virtual std::vector<marble::Status> MultiGet(const ReadOptions& options,
                                                  ColumnFamilyHandle* cf,
                                                  const std::vector<Slice>& keys,
                                                  std::vector<std::string>* values);

    //==========================================================================
    // Iteration
    //==========================================================================

    // Create an iterator
    virtual Iterator* NewIterator(const ReadOptions& options);
    virtual Iterator* NewIterator(const ReadOptions& options, ColumnFamilyHandle* cf);

    //==========================================================================
    // Column families
    //==========================================================================

    // Create column family
    virtual marble::Status CreateColumnFamily(const ColumnFamilyOptions& options,
                                              const std::string& name,
                                              ColumnFamilyHandle** handle);

    // Drop column family
    virtual marble::Status DropColumnFamily(ColumnFamilyHandle* handle);

    // List column families
    virtual std::vector<std::string> ListColumnFamilies() const;

    //==========================================================================
    // Maintenance
    //==========================================================================

    // Flush
    virtual marble::Status Flush(const FlushOptions& options);
    virtual marble::Status Flush(const FlushOptions& options, ColumnFamilyHandle* cf);

    // Compact range
    virtual marble::Status CompactRange(const CompactRangeOptions& options,
                                        ColumnFamilyHandle* cf,
                                        const Slice* begin,
                                        const Slice* end);

    // Get property
    virtual bool GetProperty(const Slice& property, std::string* value);
    virtual bool GetProperty(ColumnFamilyHandle* cf, const Slice& property, std::string* value);

    //==========================================================================
    // Snapshot support
    //==========================================================================

    // Get snapshot
    virtual const void* GetSnapshot();

    // Release snapshot
    virtual void ReleaseSnapshot(const void* snapshot);

protected:
    DB() = default;

    // Implementation pointer
    std::unique_ptr<marble::MarbleDB> impl_;

    // Cached schema to avoid repeated allocations
    std::shared_ptr<arrow::Schema> default_schema_;
};

//==============================================================================
// Helper functions
//==============================================================================

// Convert MarbleDB status to string
inline std::string StatusToString(const marble::Status& status) {
    if (status.ok()) return "OK";
    return status.message();
}

}  // namespace rocksdb
}  // namespace marble
