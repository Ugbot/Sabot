#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <deque>
#include <marble/status.h>
#include <marble/memtable.h>
#include <marble/arrow_batch_memtable.h>
#include <marble/sstable.h>
#include <marble/wal.h>

namespace marble {

// Forward declaration
class Version;

/**
 * @brief LSM Tree configuration parameters
 */
struct LSMTreeConfig {
    // MemTable settings
    size_t memtable_max_size_bytes = 64 * 1024 * 1024;  // 64MB
    size_t memtable_max_entries = 1000000;             // 1M entries

    // SSTable settings
    size_t sstable_max_size_bytes = 128 * 1024 * 1024; // 128MB
    size_t sstable_block_size = 4096;                  // 4KB blocks (legacy)
    double sstable_bloom_filter_fp_rate = 0.01;        // 1% false positive rate

    // Step 2: Large Block Write Settings
    size_t write_block_size_mb = 8;                    // 8MiB write blocks for better throughput
    size_t flush_size_kb = 128;                        // 128KB flush size (NVMe optimal)
    bool enable_large_block_writes = false;            // Enable large block writes (Step 2)

    // Step 3: Write Buffer Back-Pressure Settings (NEW)
    size_t max_write_buffer_mb = 128;                  // Maximum write buffer memory (128MB)
    size_t backpressure_threshold_percent = 80;        // Apply back-pressure at 80% full
    bool enable_write_backpressure = false;            // Enable back-pressure (Step 3)
    enum BackPressureStrategy {
        BLOCK,          // Block writes until space available
        SLOW_DOWN,      // Gradually slow down writes
        DROP_OLDEST     // Drop oldest buffered writes (least preferred)
    };
    BackPressureStrategy backpressure_strategy = SLOW_DOWN;  // Default strategy

    // Compaction settings
    size_t l0_compaction_trigger = 4;                  // Trigger compaction when L0 has 4 files
    size_t max_levels = 7;                             // Maximum number of levels
    size_t level_multiplier = 10;                      // Each level is 10x larger than previous

    // Background threads
    size_t compaction_threads = 2;                     // Number of compaction threads
    size_t flush_threads = 4;                          // Number of flush threads (INCREASED from 1 for parallel I/O)

    // Memory-Mapped Flush Settings (NEW - for petabyte-scale performance)
    bool enable_mmap_flush = true;                     // Enable memory-mapped flush (5-10× faster)
    size_t flush_zone_size_mb = 8;                     // Zone size in MB (8MB optimal for NVMe)
    bool use_async_msync = true;                       // Use MS_ASYNC for non-blocking sync
    size_t max_immutable_memtables = 8;                // Max queued memtables (backpressure limit)

    // Directory paths
    std::string data_directory = "./lsm_data";
    std::string wal_directory = "./lsm_wal";
    std::string temp_directory = "./lsm_temp";

    // Performance tuning
    bool enable_bloom_filters = true;
    bool enable_compression = true;
    size_t read_cache_size_mb = 128;                   // 128MB read cache
};

/**
 * @brief LSM Tree statistics
 */
struct LSMTreeStats {
    // MemTable stats
    size_t active_memtable_entries = 0;
    size_t active_memtable_size_bytes = 0;
    size_t immutable_memtables_count = 0;

    // SSTable stats
    size_t total_sstables = 0;
    size_t total_sstable_size_bytes = 0;
    std::vector<size_t> sstables_per_level;

    // Compaction stats
    size_t ongoing_compactions = 0;
    size_t completed_compactions = 0;
    uint64_t total_compaction_bytes = 0;

    // Flush stats
    size_t ongoing_flushes = 0;
    size_t completed_flushes = 0;

    // Performance stats
    uint64_t total_writes = 0;
    uint64_t total_reads = 0;
    uint64_t cache_hits = 0;
    uint64_t cache_misses = 0;
};

/**
 * @brief Compaction task description
 */
struct CompactionTask {
    enum Type {
        kMinorCompaction,  // MemTable -> L0
        kMajorCompaction   // Level N -> Level N+1
    };

    Type type;
    uint64_t level;        // Target level for major compaction
    std::vector<std::string> input_files;  // SSTable files to compact
    std::string output_directory;

    CompactionTask(Type t, uint64_t lvl = 0)
        : type(t), level(lvl) {}
};

/**
 * @brief LSM Tree - Log-Structured Merge Tree implementation
 *
 * Provides:
 * - High write throughput through memtable buffering
 * - Efficient reads through multi-level SSTable organization
 * - Automatic compaction to maintain performance
 * - Crash recovery through WAL integration
 */
class LSMTree {
public:
    virtual ~LSMTree() = default;

    /**
     * @brief Initialize the LSM Tree
     */
    virtual Status Init(const LSMTreeConfig& config) = 0;

    /**
     * @brief Shutdown the LSM Tree gracefully
     */
    virtual Status Shutdown() = 0;

    /**
     * @brief Put a key-value pair
     */
    virtual Status Put(uint64_t key, const std::string& value) = 0;

    /**
     * @brief Delete a key
     */
    virtual Status Delete(uint64_t key) = 0;

    /**
     * @brief Get a value by key
     */
    virtual Status Get(uint64_t key, std::string* value) const = 0;

    /**
     * @brief Check if key exists
     */
    virtual bool Contains(uint64_t key) const = 0;

    /**
     * @brief Scan a range of keys
     */
    virtual Status Scan(uint64_t start_key, uint64_t end_key,
                       std::vector<std::pair<uint64_t, std::string>>* results) = 0;

    /**
     * @brief Put an Arrow RecordBatch directly (zero-copy)
     *
     * This is the Arrow-native write path that eliminates double serialization.
     * Batches are stored directly in ArrowBatchMemTable without string conversion.
     *
     * @param batch RecordBatch to store
     * @return Status OK on success
     */
    virtual Status PutBatch(const std::shared_ptr<arrow::RecordBatch>& batch) = 0;

    /**
     * @brief Scan a range as Arrow RecordBatches (zero-copy)
     *
     * Returns data directly in Arrow format without serialization overhead.
     *
     * @param start_key Start of range (inclusive)
     * @param end_key End of range (inclusive)
     * @param batches Output vector of RecordBatches
     * @return Status OK on success
     */
    virtual Status ScanSSTablesBatches(uint64_t start_key, uint64_t end_key,
                                       std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const = 0;

    /**
     * @brief Force a memtable flush to disk
     */
    virtual Status Flush() = 0;

    /**
     * @brief Force compaction of a specific level
     */
    virtual Status Compact(uint64_t level) = 0;

    /**
     * @brief Get current statistics
     */
    virtual LSMTreeStats GetStats() const = 0;

    /**
     * @brief Get configuration
     */
    virtual const LSMTreeConfig& GetConfig() const = 0;

    /**
     * @brief Wait for all background operations to complete
     */
    virtual Status WaitForBackgroundTasks() = 0;

    /**
     * @brief Get current version for Arrow integration (internal use)
     *
     * Returns the current version which contains all SSTable levels.
     * Used internally by CreateRecordBatchReader() for Arrow integration.
     *
     * @param version Output pointer to current version
     * @return Status OK on success
     */
    virtual Status GetCurrentVersion(std::shared_ptr<Version>* version) = 0;
};

/**
 * @brief LSM Tree implementation
 */
class StandardLSMTree : public LSMTree {
public:
    StandardLSMTree();
    ~StandardLSMTree() override;

    Status Init(const LSMTreeConfig& config) override;
    Status Shutdown() override;
    Status Put(uint64_t key, const std::string& value) override;
    Status Delete(uint64_t key) override;
    Status Get(uint64_t key, std::string* value) const override;
    bool Contains(uint64_t key) const override;
    Status Scan(uint64_t start_key, uint64_t end_key,
               std::vector<std::pair<uint64_t, std::string>>* results) override;
    Status ScanSSTablesBatches(uint64_t start_key, uint64_t end_key,
                              std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const override;

    /**
     * @brief Put an Arrow RecordBatch directly (zero-copy)
     *
     * This is the Arrow-native write path that eliminates double serialization.
     * Batches are stored directly in ArrowBatchMemTable without string conversion.
     *
     * @param batch RecordBatch to store
     * @return Status OK on success
     */
    Status PutBatch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    Status Flush() override;
    Status Compact(uint64_t level) override;
    LSMTreeStats GetStats() const override;
    const LSMTreeConfig& GetConfig() const override;
    Status WaitForBackgroundTasks() override;
    Status GetCurrentVersion(std::shared_ptr<Version>* version) override;

private:
    // Core components
    LSMTreeConfig config_;
    std::unique_ptr<SimpleMemTableFactory> memtable_factory_;
    std::unique_ptr<SSTableManager> sstable_manager_;
    std::unique_ptr<WalManager> wal_manager_;
    std::shared_ptr<void> shared_node_pool_;  // Shared pool for all memtables (type-erased)

    // MemTables (using Simple interface for uint64_t keys) - LEGACY PATH
    std::unique_ptr<SimpleMemTable> active_memtable_;
    std::vector<std::unique_ptr<SimpleMemTable>> immutable_memtables_;

    // Arrow-native MemTables - NEW ZERO-COPY PATH
    std::unique_ptr<ArrowBatchMemTable> arrow_active_memtable_;
    std::vector<std::unique_ptr<ImmutableArrowBatchMemTable>> arrow_immutable_memtables_;

    // SSTables organized by level
    // NOTE: Changed from unique_ptr to shared_ptr to support Version snapshots
    // This allows Arrow RecordBatchReaders to maintain references to SSTables
    // while reading, preventing premature deletion during compaction
    std::vector<std::vector<std::shared_ptr<SSTable>>> sstables_;

    // Background task management
    std::vector<std::thread> compaction_threads_;
    std::vector<std::thread> flush_threads_;
    std::atomic<bool> shutdown_requested_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;

    // Compaction queue
    std::deque<CompactionTask> compaction_queue_;
    std::mutex compaction_mutex_;
    std::condition_variable compaction_cv_;

    // Statistics
    mutable std::mutex stats_mutex_;
    mutable LSMTreeStats stats_;

    // Internal helper methods
    Status CreateDirectories();
    Status RecoverFromDisk();
    Status SwitchMemTable();
    Status FlushMemTable(std::unique_ptr<SimpleMemTable> memtable);

    // Arrow-native helper methods (NEW)
    Status SwitchBatchMemTable();
    Status FlushBatchMemTable(std::unique_ptr<ArrowBatchMemTable> memtable);

    Status ScheduleCompaction(const CompactionTask& task);
    Status PerformCompaction(const CompactionTask& task);
    Status PerformMinorCompaction(const std::vector<std::unique_ptr<SimpleMemTable>>& memtables);
    Status PerformMajorCompaction(uint64_t level, const std::vector<std::string>& input_files);
    Status MergeSSTables(const std::vector<std::shared_ptr<SSTable>>& inputs,
                        std::shared_ptr<SSTable>* output);
    bool NeedsCompaction(uint64_t level) const;
    std::vector<std::string> SelectCompactionFiles(uint64_t level) const;

    // Background threads
    void CompactionWorker();
    void FlushWorker();

    // Read path helpers
    Status ReadFromMemTables(uint64_t key, std::string* value) const;
    Status ReadFromSSTables(uint64_t key, std::string* value) const;
    Status ScanMemTables(uint64_t start_key, uint64_t end_key,
                        std::vector<std::pair<uint64_t, std::string>>* results) const;
    Status ScanSSTables(uint64_t start_key, uint64_t end_key,
                       std::vector<std::pair<uint64_t, std::string>>* results) const;
};

/**
 * @brief Compaction strategy interface
 */
class CompactionStrategy {
public:
    virtual ~CompactionStrategy() = default;

    /**
     * @brief Determine if a level needs compaction
     */
    virtual bool NeedsCompaction(const std::vector<std::shared_ptr<SSTable>>& level_files,
                                const LSMTreeConfig& config) const = 0;

    /**
     * @brief Select files for compaction
     */
    virtual std::vector<size_t> SelectFilesForCompaction(
        const std::vector<std::shared_ptr<SSTable>>& level_files,
        const LSMTreeConfig& config) const = 0;
};

/**
 * @brief Leveled compaction strategy (RocksDB-style)
 */
class LeveledCompactionStrategy : public CompactionStrategy {
public:
    bool NeedsCompaction(const std::vector<std::shared_ptr<SSTable>>& level_files,
                        const LSMTreeConfig& config) const override;

    std::vector<size_t> SelectFilesForCompaction(
        const std::vector<std::shared_ptr<SSTable>>& level_files,
        const LSMTreeConfig& config) const override;
};

/**
 * @brief Size-tiered compaction strategy (Cassandra-style)
 */
class SizeTieredCompactionStrategy : public CompactionStrategy {
public:
    bool NeedsCompaction(const std::vector<std::shared_ptr<SSTable>>& level_files,
                        const LSMTreeConfig& config) const override;

    std::vector<size_t> SelectFilesForCompaction(
        const std::vector<std::shared_ptr<SSTable>>& level_files,
        const LSMTreeConfig& config) const override;
};

// Factory functions
std::unique_ptr<LSMTree> CreateLSMTree();
std::unique_ptr<CompactionStrategy> CreateLeveledCompactionStrategy();
std::unique_ptr<CompactionStrategy> CreateSizeTieredCompactionStrategy();

} // namespace marble

