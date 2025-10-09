#pragma once

#include <memory>
#include <vector>
#include <string>
#include <functional>
#include <atomic>
#include <marble/status.h>
#include <marble/task_scheduler.h>
#include <marble/sstable.h>

namespace marble {

// Forward declarations
class LSMTree;
class ImmutableMemTable;
class TaskScheduler;

// Compaction task representing work to be done
struct CompactionTask {
    enum class Type {
        kMinor,      // MemTable to L0 SST
        kMajor,      // SST level merging
        kTierMerge   // Tier merging (tiered compaction)
    };

    Type type;
    int source_level;
    int target_level;
    std::vector<std::shared_ptr<SSTable>> input_tables;
    // Note: We can't store unique_ptrs in a struct that needs to be copyable
    // In real implementation, this would be handled differently
    std::vector<std::shared_ptr<ImmutableMemTable>> input_memtables;

    // For tiered compaction
    int source_tier = 0;
    int target_tier = 0;

    CompactionTask(Type t, int src_level, int tgt_level = -1)
        : type(t), source_level(src_level), target_level(tgt_level) {}
};

// Compaction statistics
struct CompactionStats {
    uint64_t total_compactions = 0;
    uint64_t bytes_read = 0;
    uint64_t bytes_written = 0;
    uint64_t files_compacted = 0;
    uint64_t files_created = 0;
    double avg_compaction_time_ms = 0.0;

    std::string ToString() const;
};

// Abstract compaction strategy interface
class CompactionStrategy {
public:
    virtual ~CompactionStrategy() = default;

    // Check if compaction is needed and return tasks to perform
    virtual Status CheckCompactionNeeded(
        const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels,
        const std::vector<std::unique_ptr<ImmutableMemTable>>& immutable_memtables,
        std::vector<CompactionTask>* tasks) const = 0;

    // Get strategy name for debugging
    virtual std::string GetName() const = 0;

    // Get strategy-specific options
    virtual std::string GetOptions() const = 0;

    // Clone the strategy (for thread safety)
    virtual std::unique_ptr<CompactionStrategy> Clone() const = 0;
};

// Options for leveled compaction strategy
struct LeveledCompactionOptions {
    // Maximum number of L0 files before triggering compaction
    size_t max_l0_files = 4;

    // Size multiplier between levels (L(n+1) = L(n) * multiplier)
    size_t level_multiplier = 10;

    // Maximum size for L0 files
    size_t max_l0_file_size = 64 * 1024 * 1024;  // 64MB

    // Maximum size for files in higher levels
    size_t max_file_size = 256 * 1024 * 1024;  // 256MB

    // Number of levels
    size_t num_levels = 7;
};

// Leveled compaction strategy (similar to RocksDB leveled compaction)
class LeveledCompactionStrategy : public CompactionStrategy {
public:
    explicit LeveledCompactionStrategy(const LeveledCompactionOptions& options = LeveledCompactionOptions());

    Status CheckCompactionNeeded(
        const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels,
        const std::vector<std::unique_ptr<ImmutableMemTable>>& immutable_memtables,
        std::vector<CompactionTask>* tasks) const override;

    std::string GetName() const override { return "Leveled"; }
    std::string GetOptions() const override;

    std::unique_ptr<CompactionStrategy> Clone() const override {
        return std::make_unique<LeveledCompactionStrategy>(*this);
    }

private:
    LeveledCompactionOptions options_;

    // Check if L0 compaction is needed
    bool NeedsL0Compaction(
        const std::vector<std::shared_ptr<SSTable>>& l0_files) const;

    // Find overlapping files in a level for a given key range
    std::vector<std::shared_ptr<SSTable>> FindOverlappingFiles(
        const std::vector<std::shared_ptr<SSTable>>& level_files,
        const std::string& min_key,
        const std::string& max_key) const;
};

// Options for tiered compaction strategy
struct TieredCompactionOptions {
    // Maximum number of SSTs per tier
    size_t max_ssts_per_tier = 4;

    // Minimum number of SSTs to trigger tiered compaction
    size_t min_ssts_for_compaction = 4;

    // Maximum number of tiers
    size_t max_tiers = 4;

    // Growth factor between tiers (tier N+1 = tier N * factor)
    size_t tier_growth_factor = 4;
};

// Tiered compaction strategy (similar to Cassandra tiered compaction)
class TieredCompactionStrategy : public CompactionStrategy {
public:
    explicit TieredCompactionStrategy(const TieredCompactionOptions& options = TieredCompactionOptions());

    Status CheckCompactionNeeded(
        const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels,
        const std::vector<std::unique_ptr<ImmutableMemTable>>& immutable_memtables,
        std::vector<CompactionTask>* tasks) const override;

    std::string GetName() const override { return "Tiered"; }
    std::string GetOptions() const override;

    std::unique_ptr<CompactionStrategy> Clone() const override {
        return std::make_unique<TieredCompactionStrategy>(*this);
    }

private:
    TieredCompactionOptions options_;

    // Check if tiered compaction is needed for a level
    bool NeedsTieredCompaction(
        const std::vector<std::shared_ptr<SSTable>>& level_files) const;

    // Group files into tiers based on size/age
    std::vector<std::vector<std::shared_ptr<SSTable>>> GroupIntoTiers(
        const std::vector<std::shared_ptr<SSTable>>& level_files) const;
};

// Options for universal compaction strategy
struct UniversalCompactionOptions {
    // Size ratio for universal compaction
    double size_ratio = 1.0;

    // Maximum number of files in a compaction
    size_t max_merge_width = 10;

    // Minimum number of files to compact together
    size_t min_merge_width = 2;

    // Maximum size amplification
    double max_size_amplification = 1.5;
};

// Universal compaction strategy (adaptive, chooses between leveled/tiered)
class UniversalCompactionStrategy : public CompactionStrategy {
public:
    explicit UniversalCompactionStrategy(const UniversalCompactionOptions& options = UniversalCompactionOptions());

    Status CheckCompactionNeeded(
        const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels,
        const std::vector<std::unique_ptr<ImmutableMemTable>>& immutable_memtables,
        std::vector<CompactionTask>* tasks) const override;

    std::string GetName() const override { return "Universal"; }
    std::string GetOptions() const override;

    std::unique_ptr<CompactionStrategy> Clone() const override {
        return std::make_unique<UniversalCompactionStrategy>(*this);
    }

private:
    UniversalCompactionOptions options_;
};

// Compaction executor that manages background compaction tasks
class CompactionExecutor {
public:
    explicit CompactionExecutor(std::unique_ptr<CompactionStrategy> strategy);

    // Submit compaction tasks for execution
    Status SubmitCompaction(const CompactionTask& task,
                           std::function<void(Status)> callback = nullptr);

    // Get current compaction statistics
    CompactionStats GetStats() const;

    // Pause/resume compaction
    void Pause();
    void Resume();
    bool IsPaused() const;

    // Change compaction strategy
    Status SetStrategy(std::unique_ptr<CompactionStrategy> strategy);

private:
    std::unique_ptr<CompactionStrategy> strategy_;
    std::atomic<CompactionStats> stats_;
    std::atomic<bool> paused_;

    // Execute a single compaction task
    void ExecuteCompaction(const CompactionTask& task,
                          std::function<void(Status)> callback);
};

// Factory functions
std::unique_ptr<CompactionStrategy> CreateLeveledCompactionStrategy(
    const LeveledCompactionOptions& options = LeveledCompactionOptions());

std::unique_ptr<CompactionStrategy> CreateTieredCompactionStrategy(
    const TieredCompactionOptions& options = TieredCompactionOptions());

std::unique_ptr<CompactionStrategy> CreateUniversalCompactionStrategy(
    const UniversalCompactionOptions& options = UniversalCompactionOptions());

std::unique_ptr<CompactionExecutor> CreateCompactionExecutor(
    std::unique_ptr<CompactionStrategy> strategy);

} // namespace marble
