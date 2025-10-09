#include <marble/compaction.h>
#include <marble/sstable.h>
#include <algorithm>
#include <chrono>
#include <sstream>
#include <iostream>

namespace marble {

// CompactionStats implementation
std::string CompactionStats::ToString() const {
    std::stringstream ss;
    ss << "CompactionStats{"
       << "total_compactions=" << total_compactions
       << ", bytes_read=" << bytes_read
       << ", bytes_written=" << bytes_written
       << ", files_compacted=" << files_compacted
       << ", files_created=" << files_created
       << ", avg_compaction_time_ms=" << avg_compaction_time_ms
       << "}";
    return ss.str();
}

// LeveledCompactionStrategy implementation
LeveledCompactionStrategy::LeveledCompactionStrategy(const LeveledCompactionOptions& options)
    : options_(options) {}

Status LeveledCompactionStrategy::CheckCompactionNeeded(
    const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels,
    const std::vector<std::unique_ptr<ImmutableMemTable>>& immutable_memtables,
    std::vector<CompactionTask>* tasks) const {

    // First, check for minor compaction (memtables to L0)
    if (!immutable_memtables.empty()) {
        CompactionTask minor_task(CompactionTask::Type::kMinor, -1, 0);
        for (const auto& memtable : immutable_memtables) {
            // We can't directly move unique_ptrs, but we can create a reference
            // In real implementation, we'd need to handle this differently
        }
        tasks->push_back(minor_task);
    }

    // Check L0 compaction
    if (levels.size() > 0 && NeedsL0Compaction(levels[0])) {
        CompactionTask l0_task(CompactionTask::Type::kMajor, 0, 1);
        l0_task.input_tables = levels[0];  // Compact all L0 files
        tasks->push_back(l0_task);
    }

    // Check higher level compactions
    for (size_t level = 1; level < levels.size() && level + 1 < options_.num_levels; ++level) {
        const auto& current_level = levels[level];
        if (current_level.empty()) continue;

        // Simple size-based triggering for higher levels
        size_t total_size = 0;
        for (const auto& table : current_level) {
            total_size += table->GetMetadata().file_size;
        }

        size_t max_size_for_level = options_.max_file_size *
                                   std::pow(options_.level_multiplier, level);

        if (total_size > max_size_for_level) {
            CompactionTask major_task(CompactionTask::Type::kMajor, level, level + 1);
            major_task.input_tables = current_level;
            tasks->push_back(major_task);
        }
    }

    return Status::OK();
}

std::string LeveledCompactionStrategy::GetOptions() const {
    std::stringstream ss;
    ss << "max_l0_files=" << options_.max_l0_files
       << ", level_multiplier=" << options_.level_multiplier
       << ", max_l0_file_size=" << options_.max_l0_file_size
       << ", max_file_size=" << options_.max_file_size
       << ", num_levels=" << options_.num_levels;
    return ss.str();
}

bool LeveledCompactionStrategy::NeedsL0Compaction(
    const std::vector<std::shared_ptr<SSTable>>& l0_files) const {
    return l0_files.size() >= options_.max_l0_files;
}

std::vector<std::shared_ptr<SSTable>> LeveledCompactionStrategy::FindOverlappingFiles(
    const std::vector<std::shared_ptr<SSTable>>& level_files,
    const std::string& min_key,
    const std::string& max_key) const {

    std::vector<std::shared_ptr<SSTable>> overlapping;

    for (const auto& table : level_files) {
        const auto& meta = table->GetMetadata();
        // Simple key range comparison for compaction
        // Convert string keys to uint64_t for comparison (simplified)
        uint64_t min_key_num = std::stoull(min_key);
        uint64_t max_key_num = std::stoull(max_key);
        if (!(max_key_num < meta.min_key || meta.max_key < min_key_num)) {
            overlapping.push_back(table);
        }
    }

    return overlapping;
}

// TieredCompactionStrategy implementation
TieredCompactionStrategy::TieredCompactionStrategy(const TieredCompactionOptions& options)
    : options_(options) {}

Status TieredCompactionStrategy::CheckCompactionNeeded(
    const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels,
    const std::vector<std::unique_ptr<ImmutableMemTable>>& immutable_memtables,
    std::vector<CompactionTask>* tasks) const {

    // Minor compaction first
    if (!immutable_memtables.empty()) {
        CompactionTask minor_task(CompactionTask::Type::kMinor, -1, 0);
        tasks->push_back(minor_task);
    }

    // Check each level for tiered compaction
    for (size_t level = 0; level < levels.size(); ++level) {
        const auto& level_files = levels[level];
        if (NeedsTieredCompaction(level_files)) {
            auto tiers = GroupIntoTiers(level_files);

            // For tiered compaction, merge within the same level
            for (size_t tier = 0; tier + 1 < tiers.size(); ++tier) {
                if (!tiers[tier].empty() && !tiers[tier + 1].empty()) {
                    CompactionTask tier_task(CompactionTask::Type::kTierMerge, level, level);
                    tier_task.input_tables = tiers[tier];
                    // Add some files from next tier
                    auto next_tier_begin = tiers[tier + 1].begin();
                    auto next_tier_end = next_tier_begin + std::min(size_t(2), tiers[tier + 1].size());
                    tier_task.input_tables.insert(
                        tier_task.input_tables.end(), next_tier_begin, next_tier_end);

                    tier_task.source_tier = tier;
                    tier_task.target_tier = tier + 1;
                    tasks->push_back(tier_task);
                    break; // Only one compaction per level at a time
                }
            }
        }
    }

    return Status::OK();
}

std::string TieredCompactionStrategy::GetOptions() const {
    std::stringstream ss;
    ss << "max_ssts_per_tier=" << options_.max_ssts_per_tier
       << ", min_ssts_for_compaction=" << options_.min_ssts_for_compaction
       << ", max_tiers=" << options_.max_tiers
       << ", tier_growth_factor=" << options_.tier_growth_factor;
    return ss.str();
}

bool TieredCompactionStrategy::NeedsTieredCompaction(
    const std::vector<std::shared_ptr<SSTable>>& level_files) const {
    return level_files.size() >= options_.min_ssts_for_compaction;
}

std::vector<std::vector<std::shared_ptr<SSTable>>> TieredCompactionStrategy::GroupIntoTiers(
    const std::vector<std::shared_ptr<SSTable>>& level_files) const {

    std::vector<std::vector<std::shared_ptr<SSTable>>> tiers;
    if (level_files.empty()) return tiers;

    // Simple tier grouping based on file size (smaller files in lower tiers)
    std::vector<std::shared_ptr<SSTable>> sorted_files = level_files;
    std::sort(sorted_files.begin(), sorted_files.end(),
              [](const std::shared_ptr<SSTable>& a, const std::shared_ptr<SSTable>& b) {
                  return a->GetMetadata().file_size < b->GetMetadata().file_size;
              });

    size_t tier_size = options_.max_ssts_per_tier;
    for (size_t i = 0; i < sorted_files.size(); i += tier_size) {
        size_t end = std::min(i + tier_size, sorted_files.size());
        tiers.emplace_back(sorted_files.begin() + i, sorted_files.begin() + end);

        // Increase tier size for next tier
        tier_size *= options_.tier_growth_factor;
        if (tiers.size() >= options_.max_tiers) break;
    }

    return tiers;
}

// UniversalCompactionStrategy implementation
UniversalCompactionStrategy::UniversalCompactionStrategy(const UniversalCompactionOptions& options)
    : options_(options) {}

Status UniversalCompactionStrategy::CheckCompactionNeeded(
    const std::vector<std::vector<std::shared_ptr<SSTable>>>& levels,
    const std::vector<std::unique_ptr<ImmutableMemTable>>& immutable_memtables,
    std::vector<CompactionTask>* tasks) const {

    // Minor compaction
    if (!immutable_memtables.empty()) {
        CompactionTask minor_task(CompactionTask::Type::kMinor, -1, 0);
        tasks->push_back(minor_task);
    }

    // Universal compaction logic - adaptively choose what to compact
    for (size_t level = 0; level < levels.size(); ++level) {
        const auto& level_files = levels[level];
        if (level_files.size() < options_.min_merge_width) continue;

        // Calculate total size and find candidate files
        size_t total_size = 0;
        for (const auto& file : level_files) {
            total_size += file->GetMetadata().file_size;
        }

        // Check size amplification
        size_t largest_file_size = 0;
        for (const auto& file : level_files) {
            largest_file_size = std::max(largest_file_size, static_cast<size_t>(file->GetMetadata().file_size));
        }

        double size_amplification = static_cast<double>(total_size) / largest_file_size;
        if (size_amplification <= options_.max_size_amplification) {
            // Files are similar in size, good candidates for compaction
            size_t num_to_compact = std::min(level_files.size(), options_.max_merge_width);
            CompactionTask universal_task(CompactionTask::Type::kMajor, level, level + 1);
            universal_task.input_tables.assign(
                level_files.begin(), level_files.begin() + num_to_compact);
            tasks->push_back(universal_task);
        }
    }

    return Status::OK();
}

std::string UniversalCompactionStrategy::GetOptions() const {
    std::stringstream ss;
    ss << "size_ratio=" << options_.size_ratio
       << ", max_merge_width=" << options_.max_merge_width
       << ", min_merge_width=" << options_.min_merge_width
       << ", max_size_amplification=" << options_.max_size_amplification;
    return ss.str();
}

// CompactionExecutor implementation
CompactionExecutor::CompactionExecutor(std::unique_ptr<CompactionStrategy> strategy)
    : strategy_(std::move(strategy)), paused_(false) {}

Status CompactionExecutor::SubmitCompaction(const CompactionTask& task,
                                          std::function<void(Status)> callback) {
    if (paused_.load()) {
        return Status::OK();  // Silently ignore when paused
    }

    // Submit to task scheduler
    auto task_func = [this, task, callback]() {
        ExecuteCompaction(task, callback);
    };

    // In real implementation, would use scheduler_->ScheduleTask(task_func);
    // For now, execute synchronously
    ExecuteCompaction(task, callback);

    return Status::OK();
}

CompactionStats CompactionExecutor::GetStats() const {
    return stats_.load();
}

void CompactionExecutor::Pause() {
    paused_.store(true);
}

void CompactionExecutor::Resume() {
    paused_.store(false);
}

bool CompactionExecutor::IsPaused() const {
    return paused_.load();
}

Status CompactionExecutor::SetStrategy(std::unique_ptr<CompactionStrategy> strategy) {
    strategy_ = std::move(strategy);
    return Status::OK();
}

void CompactionExecutor::ExecuteCompaction(const CompactionTask& task,
                                         std::function<void(Status)> callback) {
    auto start_time = std::chrono::steady_clock::now();

    // Update stats
    CompactionStats current_stats = stats_.load();
    current_stats.total_compactions++;
    current_stats.files_compacted += task.input_tables.size();

    // Simulate compaction work
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Simulate creating output files
    current_stats.files_created += (task.input_tables.size() + 1) / 2;  // Rough estimate
    current_stats.bytes_read += task.input_tables.size() * 1024 * 1024;  // Estimate
    current_stats.bytes_written += current_stats.bytes_read / 2;  // Compression

    auto end_time = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // Update average time
    current_stats.avg_compaction_time_ms =
        (current_stats.avg_compaction_time_ms * (current_stats.total_compactions - 1) + duration_ms)
        / current_stats.total_compactions;

    stats_.store(current_stats);

    if (callback) {
        callback(Status::OK());
    }
}

// Factory functions
std::unique_ptr<CompactionStrategy> CreateLeveledCompactionStrategy(
    const LeveledCompactionOptions& options) {
    return std::make_unique<LeveledCompactionStrategy>(options);
}

std::unique_ptr<CompactionStrategy> CreateTieredCompactionStrategy(
    const TieredCompactionOptions& options) {
    return std::make_unique<TieredCompactionStrategy>(options);
}

std::unique_ptr<CompactionStrategy> CreateUniversalCompactionStrategy(
    const UniversalCompactionOptions& options) {
    return std::make_unique<UniversalCompactionStrategy>(options);
}

std::unique_ptr<CompactionExecutor> CreateCompactionExecutor(
    std::unique_ptr<CompactionStrategy> strategy) {
    return std::make_unique<CompactionExecutor>(std::move(strategy));
}

} // namespace marble
