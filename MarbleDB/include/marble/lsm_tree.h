#pragma once

#include <memory>
#include <vector>
#include <string>
#include <functional>
#include <marble/status.h>
#include <marble/record.h>
#include <marble/db.h>
#include <marble/task_scheduler.h>
#include <marble/query.h>
#include <marble/hot_key_cache.h>
#include <arrow/api.h>

namespace marble {

// Forward declarations
class MemTable;
class ImmutableMemTable;
class LSMSSTable;
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

// LSM SSTable statistics and metadata
class LSMSSTable {
public:
    LSMSSTable() = default;

    // Block-level statistics for ClickHouse-style indexing
    struct BlockStats {
        std::shared_ptr<Key> min_key;
        std::shared_ptr<Key> max_key;
        uint64_t row_count = 0;
        uint64_t block_offset = 0;  // Offset in the SSTable file
        uint64_t block_size = 0;    // Size of the block in bytes
        uint64_t first_row_index = 0; // Index of first row in this block
        std::shared_ptr<BloomFilter> block_bloom;  // Per-block bloom filter (shared for copyability)
    };

    // Sparse index entry for fast key lookup
    struct SparseIndexEntry {
        std::shared_ptr<Key> key;
        uint64_t block_index = 0;   // Which block this key belongs to
        uint64_t row_index = 0;     // Row index within the block
    };

    struct Metadata {
        std::string filename;
        uint64_t file_size = 0;
        std::shared_ptr<Key> smallest_key;
        std::shared_ptr<Key> largest_key;
        uint64_t num_entries = 0;
        int level = 0;  // LSM level
        uint64_t sequence_number = 0;

        // ClickHouse-style indexing
        uint32_t block_size = 8192;  // Target block size in rows
        uint32_t index_granularity = 8192;  // Sparse index every N rows
        bool has_bloom_filter = false;
        bool has_sparse_index = false;
        std::vector<BlockStats> block_stats;
        std::vector<SparseIndexEntry> sparse_index;
    };

    virtual ~LSMSSTable() = default;

    // Open an existing SSTable
    static Status Open(const std::string& filename, std::unique_ptr<LSMSSTable>* table);


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

    // Get block-level statistics for query optimization
    virtual Status GetBlockStats(std::vector<BlockStats>* stats) const = 0;

    // Read the entire SSTable as RecordBatch
    virtual Status ReadRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch) const = 0;

    // Disable copying
    LSMSSTable(const LSMSSTable&) = delete;
    LSMSSTable& operator=(const LSMSSTable&) = delete;
};

// Factory function for creating SSTables with ClickHouse-style indexing
Status CreateSSTable(const std::string& filename,
                    const std::shared_ptr<arrow::RecordBatch>& batch,
                    const DBOptions& options,
                    std::unique_ptr<LSMSSTable>* table);

// Simple Bloom Filter implementation for ClickHouse-style indexing
class BloomFilter {
public:
    BloomFilter(size_t bits_per_key, size_t num_keys);
    ~BloomFilter();

    // Add a key to the filter
    void Add(const Key& key);

    // Check if a key might be in the filter
    bool MayContain(const Key& key) const;

    // Get the size of the filter in bytes
    size_t Size() const { return bits_.size() / 8; }

    // Serialize the filter
    Status Serialize(std::string* data) const;

    // Deserialize the filter
    static Status Deserialize(const std::string& data, std::unique_ptr<BloomFilter>* filter);

private:
    std::vector<uint8_t> bits_;
    size_t num_hashes_;

    // Hash functions for Bloom filter
    uint64_t Hash1(const std::string& key) const;
    uint64_t Hash2(const std::string& key) const;
    size_t NthHash(uint64_t hash1, uint64_t hash2, size_t n) const;
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

    // MVCC-aware read operations
    virtual Status GetForSnapshot(const Key& key, uint64_t snapshot_ts,
                                  std::shared_ptr<Record>* record) const = 0;

    // Range operations
    virtual Status Scan(const Key& start, const Key& end,
                       std::vector<std::shared_ptr<Record>>* records) const = 0;

    // MVCC-aware range operations
    virtual Status ScanForSnapshot(const Key& start, const Key& end, uint64_t snapshot_ts,
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
