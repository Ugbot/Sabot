#pragma once

#include <memory>
#include <vector>
#include <string>
#include <functional>
#include <marble/status.h>
#include <marble/record.h>
#include <marble/task_scheduler.h>
#include <marble/query.h>
#include <arrow/api.h>

namespace marble {

// Forward declarations
class MemTable;
class ImmutableMemTable;
class SSTable;
class LSMTree;
class CompactionManager;

// MemTable for in-memory writes
class MemTable {
public:
    MemTable() = default;
    virtual ~MemTable() = default;

    // Insert a record into the memtable
    virtual Status Put(std::shared_ptr<Key> key, std::shared_ptr<Record> record) = 0;

    // Get a record from the memtable
    virtual Status Get(const Key& key, std::shared_ptr<Record>* record) const = 0;

    // Delete a record from the memtable
    virtual Status Delete(std::shared_ptr<Key> key) = 0;

    // Check if memtable is full and needs to be flushed
    virtual bool IsFull() const = 0;

    // Get approximate memory usage
    virtual size_t ApproximateMemoryUsage() const = 0;

    // Get number of entries
    virtual size_t Size() const = 0;

    // Convert to immutable memtable for compaction
    virtual std::unique_ptr<ImmutableMemTable> Freeze() = 0;

    // Clear all entries
    virtual void Clear() = 0;

    // Iterator interface for scanning
    class Iterator {
    public:
        virtual ~Iterator() = default;
        virtual bool Valid() const = 0;
        virtual void SeekToFirst() = 0;
        virtual void SeekToLast() = 0;
        virtual void Seek(const Key& key) = 0;
        virtual void Next() = 0;
        virtual void Prev() = 0;
        virtual std::shared_ptr<marble::Key> GetKey() const = 0;
    virtual std::shared_ptr<Record> Value() const = 0;
    virtual std::unique_ptr<RecordRef> ValueRef() const = 0;
    virtual marble::Status GetStatus() const = 0;
    };

    virtual std::unique_ptr<Iterator> NewIterator() = 0;

    // Disable copying
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;
};

// Immutable version of MemTable for compaction
class ImmutableMemTable {
public:
    ImmutableMemTable() = default;
    virtual ~ImmutableMemTable() = default;

    // Get a record from the immutable memtable
    virtual Status Get(const Key& key, std::shared_ptr<Record>* record) const = 0;

    // Iterator interface
    virtual std::unique_ptr<MemTable::Iterator> NewIterator() = 0;

    // Get approximate memory usage
    virtual size_t ApproximateMemoryUsage() const = 0;

    // Get number of entries
    virtual size_t Size() const = 0;

    // Convert to Arrow RecordBatch for persistence
    virtual Status ToRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch) const = 0;

    // Disable copying
    ImmutableMemTable(const ImmutableMemTable&) = delete;
    ImmutableMemTable& operator=(const ImmutableMemTable&) = delete;
};

// SSTable (Sorted String Table) for on-disk storage
class SSTable {
public:
    SSTable() = default;
    struct Metadata {
        std::string filename;
        uint64_t file_size = 0;
        std::shared_ptr<Key> smallest_key;
        std::shared_ptr<Key> largest_key;
        uint64_t num_entries = 0;
        int level = 0;  // LSM level
        uint64_t sequence_number = 0;
    };

    virtual ~SSTable() = default;

    // Open an existing SSTable
    static Status Open(const std::string& filename, std::unique_ptr<SSTable>* table);

    // Create a new SSTable from RecordBatch
    static Status Create(const std::string& filename,
                        const std::shared_ptr<arrow::RecordBatch>& batch,
                        std::unique_ptr<SSTable>* table);

    // Get metadata
    virtual const Metadata& GetMetadata() const = 0;

    // Get a record from the SSTable
    virtual Status Get(const Key& key, std::shared_ptr<Record>* record) const = 0;

    // Iterator interface for scanning ranges
    virtual std::unique_ptr<MemTable::Iterator> NewIterator() = 0;

    // Iterator interface with predicate filtering
    virtual std::unique_ptr<MemTable::Iterator> NewIterator(const std::vector<ColumnPredicate>& predicates) = 0;

    // Check if key might exist in this SSTable
    virtual bool KeyMayMatch(const Key& key) const = 0;

    // Get bloom filter (if available)
    virtual Status GetBloomFilter(std::string* bloom_filter) const = 0;

    // Read the entire SSTable as RecordBatch
    virtual Status ReadRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch) const = 0;

    // Disable copying
    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;
};

// LSM Tree configuration
struct LSMTreeOptions {
    // MemTable options
    size_t memtable_size_threshold = 64 * 1024 * 1024;  // 64MB
    size_t max_memtables = 4;

    // SSTable options
    size_t sstable_size_threshold = 256 * 1024 * 1024;  // 256MB
    size_t block_size = 64 * 1024;  // 64KB
    bool use_bloom_filters = true;
    bool use_compression = true;

    // Compaction options
    size_t max_level0_files = 4;
    size_t max_level_files_base = 8;
    size_t level_multiplier = 2;

    // Threading options
    size_t max_background_threads = 4;
    size_t compaction_threads = 2;

    // Storage options
    std::string db_path = "/tmp/marble";
    bool enable_wal = true;
};

// LSM Tree main interface
class LSMTree {
public:
    virtual ~LSMTree() = default;

    // Create a new LSM tree
    static Status Create(const LSMTreeOptions& options,
                        std::shared_ptr<Schema> schema,
                        std::unique_ptr<LSMTree>* tree);

    // Open an existing LSM tree
    static Status Open(const LSMTreeOptions& options,
                      std::shared_ptr<Schema> schema,
                      std::unique_ptr<LSMTree>* tree);

    // Write operations
    virtual Status Put(std::shared_ptr<Key> key, std::shared_ptr<Record> record) = 0;
    virtual Status Delete(std::shared_ptr<Key> key) = 0;

    // Read operations
    virtual Status Get(const Key& key, std::shared_ptr<Record>* record) const = 0;

    // Range operations
    virtual Status Scan(const Key& start, const Key& end,
                       std::vector<std::shared_ptr<Record>>* records) const = 0;

    // Flush current memtable to disk
    virtual Status Flush() = 0;

    // Force compaction
    virtual Status Compact() = 0;

    // Get statistics
    virtual Status GetStats(std::string* stats) const = 0;

    // Close the LSM tree
    virtual Status Close() = 0;

    // Iterator for full table scan
    virtual std::unique_ptr<MemTable::Iterator> NewIterator() = 0;

    // Get compaction manager
    virtual CompactionManager* GetCompactionManager() = 0;

    // Disable copying
    LSMTree(const LSMTree&) = delete;
    LSMTree& operator=(const LSMTree&) = delete;
};

// Compaction manager interface
class CompactionManager {
public:
    virtual ~CompactionManager() = default;

    // Check if compaction is needed and trigger it
    virtual Status CheckAndCompact() = 0;

    // Manual compaction trigger
    virtual Status CompactRange(const Key& start, const Key& end) = 0;

    // Get compaction statistics
    virtual Status GetCompactionStats(std::string* stats) const = 0;

    // Set compaction strategy
    enum class CompactionStrategy {
        kLeveled,
        kTiered,
        kUniversal
    };
    virtual Status SetCompactionStrategy(CompactionStrategy strategy) = 0;

    // Pause/Resume compaction
    virtual Status PauseCompaction() = 0;
    virtual Status ResumeCompaction() = 0;

    // Disable copying
    CompactionManager(const CompactionManager&) = delete;
    CompactionManager& operator=(const CompactionManager&) = delete;
};

// Factory functions
std::unique_ptr<MemTable> CreateSkipListMemTable(std::shared_ptr<Schema> schema);
std::unique_ptr<CompactionManager> CreateLeveledCompactionManager(
    const LSMTreeOptions& options, TaskScheduler* scheduler);

} // namespace marble
