#include "marble/lsm_storage.h"
#include <filesystem>
#include <algorithm>
#include <chrono>

namespace marble {

// StandardLSMTree implementation
StandardLSMTree::StandardLSMTree()
    : shutdown_requested_(false) {}

StandardLSMTree::~StandardLSMTree() {
    Shutdown();
}

Status StandardLSMTree::Init(const LSMTreeConfig& config) {
    config_ = config;

    // Create directory structure
    auto status = CreateDirectories();
    if (!status.ok()) return status;

    // Initialize components
    memtable_factory_ = CreateMemTableFactory();
    sstable_manager_ = CreateSSTableManager();

    // Create initial memtable
    active_memtable_ = memtable_factory_->CreateMemTable();

    // Start background threads
    compaction_threads_.reserve(config_.compaction_threads);
    flush_threads_.reserve(config_.flush_threads);

    for (size_t i = 0; i < config_.compaction_threads; ++i) {
        compaction_threads_.emplace_back(&StandardLSMTree::CompactionWorker, this);
    }

    for (size_t i = 0; i < config_.flush_threads; ++i) {
        flush_threads_.emplace_back(&StandardLSMTree::FlushWorker, this);
    }

    // Recover from existing data
    status = RecoverFromDisk();
    if (!status.ok()) return status;

    return Status::OK();
}

Status StandardLSMTree::Shutdown() {
    shutdown_requested_ = true;

    // Wake up background threads
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_all();
    }

    compaction_cv_.notify_all();

    // Wait for threads to finish
    for (auto& thread : compaction_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    for (auto& thread : flush_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    compaction_threads_.clear();
    flush_threads_.clear();

    // Final flush
    if (active_memtable_ && active_memtable_->GetEntryCount() > 0) {
        Flush();
    }

    return Status::OK();
}

Status StandardLSMTree::Put(uint64_t key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if memtable needs to be flushed
    if (active_memtable_->ShouldFlush(config_.memtable_max_size_bytes)) {
        auto status = SwitchMemTable();
        if (!status.ok()) return status;
    }

    // Write to active memtable
    auto status = active_memtable_->Put(key, value);
    if (!status.ok()) return status;

    stats_.total_writes++;
    return Status::OK();
}

Status StandardLSMTree::Delete(uint64_t key) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if memtable needs to be flushed
    if (active_memtable_->ShouldFlush(config_.memtable_max_size_bytes)) {
        auto status = SwitchMemTable();
        if (!status.ok()) return status;
    }

    // Write delete tombstone to active memtable
    auto status = active_memtable_->Delete(key);
    if (!status.ok()) return status;

    stats_.total_writes++;
    return Status::OK();
}

Status StandardLSMTree::Get(uint64_t key, std::string* value) const {
    // Search order: active memtable -> immutable memtables -> SSTables

    // Check active memtable
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto status = ReadFromMemTables(key, value);
        if (status.ok()) {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.total_reads++;
            return Status::OK();
        }
        if (status.code() != StatusCode::kNotFound) {
            return status;
        }
    }

    // Check SSTables
    auto status = ReadFromSSTables(key, value);
    if (status.ok()) {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.total_reads++;
    }
    return status;
}

bool StandardLSMTree::Contains(uint64_t key) const {
    std::string dummy_value;
    return Get(key, &dummy_value).ok();
}

Status StandardLSMTree::Scan(uint64_t start_key, uint64_t end_key,
                            std::vector<std::pair<uint64_t, std::string>>* results) {
    // Scan order: SSTables -> memtables (reverse order for recency)

    // Scan SSTables first
    auto status = ScanSSTables(start_key, end_key, results);
    if (!status.ok()) return status;

    // Scan memtables and merge results
    std::vector<std::pair<uint64_t, std::string>> memtable_results;
    status = ScanMemTables(start_key, end_key, &memtable_results);
    if (!status.ok()) return status;

    // Merge results (memtable results take precedence)
    std::map<uint64_t, std::string> merged_results;

    // Add SSTable results
    for (const auto& result : *results) {
        merged_results[result.first] = result.second;
    }

    // Add/override with memtable results
    for (const auto& result : memtable_results) {
        merged_results[result.first] = result.second;
    }

    // Convert back to vector
    results->clear();
    for (const auto& result : merged_results) {
        results->emplace_back(result.first, result.second);
    }

    return Status::OK();
}

Status StandardLSMTree::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!active_memtable_ || active_memtable_->GetEntryCount() == 0) {
        return Status::OK();
    }

    return SwitchMemTable();
}

Status StandardLSMTree::Compact(uint64_t level) {
    CompactionTask task(CompactionTask::kMajorCompaction, level);
    return ScheduleCompaction(task);
}

LSMTreeStats StandardLSMTree::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::lock_guard<std::mutex> stats_lock(stats_mutex_);

    LSMTreeStats current_stats = stats_;

    // Update live statistics
    if (active_memtable_) {
        uint64_t min_key, max_key;
        size_t entry_count, memory_usage;
        active_memtable_->GetStats(&min_key, &max_key, &entry_count, &memory_usage);
        current_stats.active_memtable_entries = entry_count;
        current_stats.active_memtable_size_bytes = memory_usage;
    }

    current_stats.immutable_memtables_count = immutable_memtables_.size();

    // Count SSTables per level
    current_stats.sstables_per_level.resize(config_.max_levels, 0);
    for (size_t level = 0; level < sstables_.size(); ++level) {
        current_stats.sstables_per_level[level] = sstables_[level].size();
    }

    current_stats.total_sstables = 0;
    current_stats.total_sstable_size_bytes = 0;
    for (const auto& level_sstables : sstables_) {
        for (const auto& sstable : level_sstables) {
            current_stats.total_sstables++;
            current_stats.total_sstable_size_bytes += sstable->GetFileSize();
        }
    }

    return current_stats;
}

const LSMTreeConfig& StandardLSMTree::GetConfig() const {
    return config_;
}

Status StandardLSMTree::WaitForBackgroundTasks() {
    // Wait for compaction queue to be empty
    while (true) {
        {
            std::lock_guard<std::mutex> lock(compaction_mutex_);
            if (compaction_queue_.empty() && stats_.ongoing_compactions == 0) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return Status::OK();
}

// Private methods
Status StandardLSMTree::CreateDirectories() {
    std::filesystem::create_directories(config_.data_directory);
    std::filesystem::create_directories(config_.wal_directory);
    std::filesystem::create_directories(config_.temp_directory);
    return Status::OK();
}

Status StandardLSMTree::RecoverFromDisk() {
    // List all SSTable files
    std::vector<std::string> sstable_files;
    auto status = sstable_manager_->ListSSTables(config_.data_directory, &sstable_files);
    if (!status.ok()) return status;

    // Load SSTables into appropriate levels
    sstables_.resize(config_.max_levels);
    for (const auto& filename : sstable_files) {
        std::string filepath = config_.data_directory + "/" + filename;

        std::unique_ptr<SSTable> sstable;
        status = sstable_manager_->OpenSSTable(filepath, &sstable);
        if (!status.ok()) {
            // Log error but continue
            continue;
        }

        uint64_t level = sstable->GetMetadata().level;
        if (level < sstables_.size()) {
            sstables_[level].push_back(std::move(sstable));
        }
    }

    return Status::OK();
}

Status StandardLSMTree::SwitchMemTable() {
    // Create new active memtable
    auto new_memtable = memtable_factory_->CreateMemTable();

    // Move current memtable to immutable list
    immutable_memtables_.push_back(std::move(active_memtable_));
    active_memtable_ = std::move(new_memtable);

    // Schedule flush of the oldest immutable memtable
    if (!immutable_memtables_.empty()) {
        // Trigger flush worker
        cv_.notify_one();
    }

    return Status::OK();
}

Status StandardLSMTree::FlushMemTable(std::unique_ptr<MemTable> memtable) {
    // Get entries from memtable
    std::vector<MemTableEntry> entries;
    auto status = memtable->GetAllEntries(&entries);
    if (!status.ok()) return status;

    if (entries.empty()) {
        return Status::OK();
    }

    // Create SSTable filename
    std::string filename = "sstable_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
    std::string filepath = config_.data_directory + "/" + filename;

    // Create SSTable writer
    std::unique_ptr<SSTableWriter> writer;
    status = sstable_manager_->CreateWriter(filepath, 0, &writer); // Start at L0
    if (!status.ok()) return status;

    // Write entries to SSTable
    for (const auto& entry : entries) {
        if (entry.op == MemTableEntry::kPut) {
            status = writer->Add(entry.key, entry.value);
            if (!status.ok()) return status;
        }
        // Skip delete entries for now (they become tombstones in SSTable)
    }

    // Finish SSTable
    std::unique_ptr<SSTable> sstable;
    status = writer->Finish(&sstable);
    if (!status.ok()) return status;

    // Add to L0
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (sstables_.size() <= 0) {
            sstables_.resize(1);
        }
        sstables_[0].push_back(std::move(sstable));

        // Check if L0 needs compaction
        if (NeedsCompaction(0)) {
            CompactionTask task(CompactionTask::kMajorCompaction, 0);
            ScheduleCompaction(task);
        }
    }

    return Status::OK();
}

Status StandardLSMTree::ScheduleCompaction(const CompactionTask& task) {
    {
        std::lock_guard<std::mutex> lock(compaction_mutex_);
        compaction_queue_.push_back(task);
    }
    compaction_cv_.notify_one();
    return Status::OK();
}

Status StandardLSMTree::PerformCompaction(const CompactionTask& task) {
    switch (task.type) {
        case CompactionTask::kMinorCompaction:
            return PerformMinorCompaction(immutable_memtables_);
        case CompactionTask::kMajorCompaction:
            return PerformMajorCompaction(task.level, task.input_files);
        default:
            return Status::InvalidArgument("Unknown compaction type");
    }
}

Status StandardLSMTree::PerformMinorCompaction(const std::vector<std::unique_ptr<MemTable>>& memtables) {
    if (memtables.empty()) {
        return Status::OK();
    }

    // For now, just flush the first memtable
    auto memtable_snapshot = memtables[0]->CreateSnapshot();
    return FlushMemTable(std::move(memtable_snapshot));
}

Status StandardLSMTree::PerformMajorCompaction(uint64_t level, const std::vector<std::string>& input_files) {
    // Select SSTables to compact
    std::vector<std::string> files_to_compact;
    if (input_files.empty()) {
        files_to_compact = SelectCompactionFiles(level);
    } else {
        files_to_compact = input_files;
    }

    if (files_to_compact.empty()) {
        return Status::OK();
    }

    // Open SSTables
    std::vector<std::unique_ptr<SSTable>> input_sstables;
    for (const auto& filepath : files_to_compact) {
        std::unique_ptr<SSTable> sstable;
        auto status = sstable_manager_->OpenSSTable(filepath, &sstable);
        if (!status.ok()) continue; // Skip corrupted files
        input_sstables.push_back(std::move(sstable));
    }

    // Merge SSTables
    std::unique_ptr<SSTable> output_sstable;
    auto status = MergeSSTables(input_sstables, &output_sstable);
    if (!status.ok()) return status;

    // Replace old SSTables with new one
    {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t target_level = level + 1;
        if (target_level >= sstables_.size()) {
            sstables_.resize(target_level + 1);
        }

        // Remove input files from current level
        auto& current_level = sstables_[level];
        for (const auto& filepath : files_to_compact) {
            auto it = std::remove_if(current_level.begin(), current_level.end(),
                                   [&](const std::unique_ptr<SSTable>& sstable) {
                                       return sstable->GetFilePath() == filepath;
                                   });
            current_level.erase(it, current_level.end());
        }

        // Add new SSTable to target level
        sstables_[target_level].push_back(std::move(output_sstable));

        // Delete old files
        for (const auto& filepath : files_to_compact) {
            sstable_manager_->DeleteSSTable(filepath);
        }
    }

    return Status::OK();
}

Status StandardLSMTree::MergeSSTables(const std::vector<std::unique_ptr<SSTable>>& inputs,
                                     std::unique_ptr<SSTable>* output) {
    if (inputs.empty()) {
        return Status::InvalidArgument("No input SSTables to merge");
    }

    // For now, just return the first SSTable
    // FIXME: Implement proper SSTable merging
    // TODO: SSTableImpl constructor not accessible due to forward declaration
    return Status::NotImplemented("SSTable merging not implemented");

    return Status::OK();
}

bool StandardLSMTree::NeedsCompaction(uint64_t level) const {
    if (level >= sstables_.size()) {
        return false;
    }

    const auto& level_sstables = sstables_[level];
    size_t file_count = level_sstables.size();

    if (level == 0) {
        return file_count >= config_.l0_compaction_trigger;
    } else {
        // For other levels, use size-based triggering
        size_t expected_files = config_.level_multiplier;
        for (size_t i = 1; i < level; ++i) {
            expected_files *= config_.level_multiplier;
        }
        return file_count > expected_files;
    }
}

std::vector<std::string> StandardLSMTree::SelectCompactionFiles(uint64_t level) const {
    if (level >= sstables_.size() || sstables_[level].empty()) {
        return {};
    }

    const auto& level_sstables = sstables_[level];
    std::vector<std::string> files;

    // For now, select all files in the level
    // FIXME: Implement smarter file selection based on size, overlap, etc.
    for (const auto& sstable : level_sstables) {
        files.push_back(sstable->GetFilePath());
    }

    return files;
}

void StandardLSMTree::CompactionWorker() {
    while (!shutdown_requested_) {
        CompactionTask task(CompactionTask::kMajorCompaction);

        {
            std::unique_lock<std::mutex> lock(compaction_mutex_);
            compaction_cv_.wait(lock, [this]() {
                return shutdown_requested_ || !compaction_queue_.empty();
            });

            if (shutdown_requested_) break;

            if (!compaction_queue_.empty()) {
                task = compaction_queue_.front();
                compaction_queue_.pop_front();
            } else {
                continue;
            }
        }

        // Perform compaction
        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.ongoing_compactions++;
        }

        auto status = PerformCompaction(task);

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.ongoing_compactions--;
            if (status.ok()) {
                stats_.completed_compactions++;
            }
        }
    }
}

void StandardLSMTree::FlushWorker() {
    while (!shutdown_requested_) {
        std::unique_ptr<MemTable> memtable_to_flush;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() {
                return shutdown_requested_ || !immutable_memtables_.empty();
            });

            if (shutdown_requested_) break;

            if (!immutable_memtables_.empty()) {
                memtable_to_flush = std::move(immutable_memtables_.front());
                immutable_memtables_.erase(immutable_memtables_.begin());
            } else {
                continue;
            }
        }

        // Flush memtable to SSTable
        FlushMemTable(std::move(memtable_to_flush));
    }
}

Status StandardLSMTree::ReadFromMemTables(uint64_t key, std::string* value) const {
    // Check active memtable
    auto status = active_memtable_->Get(key, value);
    if (status.ok()) {
        return Status::OK();
    }

    // Check immutable memtables (newest first)
    for (auto it = immutable_memtables_.rbegin(); it != immutable_memtables_.rend(); ++it) {
        status = (*it)->Get(key, value);
        if (status.ok()) {
            return Status::OK();
        }
    }

    return Status::NotFound("Key not found in memtables");
}

Status StandardLSMTree::ReadFromSSTables(uint64_t key, std::string* value) const {
    // Search SSTables from newest to oldest (L0, L1, L2, ...)
    for (size_t level = 0; level < sstables_.size(); ++level) {
        const auto& level_sstables = sstables_[level];

        // For L0, search all files (newest first)
        if (level == 0) {
            for (auto it = level_sstables.rbegin(); it != level_sstables.rend(); ++it) {
                if ((*it)->ContainsKey(key)) {
                    auto status = (*it)->Get(key, value);
                    if (status.ok()) {
                        return Status::OK();
                    }
                }
            }
        } else {
            // For other levels, SSTables are non-overlapping, so we can binary search
            // FIXME: Implement proper level-based search
            for (const auto& sstable : level_sstables) {
                if (key >= sstable->GetMetadata().min_key &&
                    key <= sstable->GetMetadata().max_key) {
                    auto status = sstable->Get(key, value);
                    if (status.ok()) {
                        return Status::OK();
                    }
                }
            }
        }
    }

    return Status::NotFound("Key not found in SSTables");
}

Status StandardLSMTree::ScanMemTables(uint64_t start_key, uint64_t end_key,
                                     std::vector<std::pair<uint64_t, std::string>>* results) const {
    // Scan active memtable
    std::vector<MemTableEntry> active_entries;
    active_memtable_->Scan(start_key, end_key, &active_entries);
    for (const auto& entry : active_entries) {
        if (entry.op == MemTableEntry::kPut) {
            results->emplace_back(entry.key, entry.value);
        }
    }

    // Scan immutable memtables
    for (const auto& memtable : immutable_memtables_) {
        std::vector<MemTableEntry> entries;
        memtable->Scan(start_key, end_key, &entries);

        for (const auto& entry : entries) {
            if (entry.op == MemTableEntry::kPut) {
                results->emplace_back(entry.key, entry.value);
            }
        }
    }

    return Status::OK();
}

Status StandardLSMTree::ScanSSTables(uint64_t start_key, uint64_t end_key,
                                    std::vector<std::pair<uint64_t, std::string>>* results) const {
    // Scan all SSTable levels
    for (size_t level = 0; level < sstables_.size(); ++level) {
        for (const auto& sstable : sstables_[level]) {
            // Check if SSTable overlaps with scan range
            const auto& metadata = sstable->GetMetadata();
            if (metadata.max_key >= start_key && metadata.min_key <= end_key) {
                std::vector<std::pair<uint64_t, std::string>> sstable_results;
                sstable->Scan(start_key, end_key, &sstable_results);
                results->insert(results->end(), sstable_results.begin(), sstable_results.end());
            }
        }
    }

    // Sort results by key (merge from multiple SSTables)
    std::sort(results->begin(), results->end());

    return Status::OK();
}

// Factory functions
std::unique_ptr<LSMTree> CreateLSMTree() {
    return std::make_unique<StandardLSMTree>();
}

std::unique_ptr<CompactionStrategy> CreateLeveledCompactionStrategy() {
    return std::make_unique<LeveledCompactionStrategy>();
}

std::unique_ptr<CompactionStrategy> CreateSizeTieredCompactionStrategy() {
    return std::make_unique<SizeTieredCompactionStrategy>();
}

// CompactionStrategy implementations
bool LeveledCompactionStrategy::NeedsCompaction(const std::vector<std::unique_ptr<SSTable>>& level_files,
                                               const LSMTreeConfig& config) const {
    return level_files.size() >= config.l0_compaction_trigger;
}

std::vector<size_t> LeveledCompactionStrategy::SelectFilesForCompaction(
    const std::vector<std::unique_ptr<SSTable>>& level_files,
    const LSMTreeConfig& config) const {
    // Select all files for compaction
    std::vector<size_t> indices;
    for (size_t i = 0; i < level_files.size(); ++i) {
        indices.push_back(i);
    }
    return indices;
}

bool SizeTieredCompactionStrategy::NeedsCompaction(const std::vector<std::unique_ptr<SSTable>>& level_files,
                                                  const LSMTreeConfig& config) const {
    if (level_files.size() < 2) {
        return false;
    }

    // Check if smallest file is much smaller than largest
    uint64_t min_size = UINT64_MAX;
    uint64_t max_size = 0;

    for (const auto& sstable : level_files) {
        uint64_t size = sstable->GetFileSize();
        min_size = std::min(min_size, size);
        max_size = std::max(max_size, size);
    }

    // Trigger compaction if size ratio is high
    return max_size > min_size * 4;
}

std::vector<size_t> SizeTieredCompactionStrategy::SelectFilesForCompaction(
    const std::vector<std::unique_ptr<SSTable>>& level_files,
    const LSMTreeConfig& config) const {
    // Select files with similar sizes
    if (level_files.empty()) return {};

    uint64_t target_size = level_files[0]->GetFileSize();
    std::vector<size_t> indices;

    for (size_t i = 0; i < level_files.size(); ++i) {
        uint64_t size = level_files[i]->GetFileSize();
        if (size >= target_size / 2 && size <= target_size * 2) {
            indices.push_back(i);
        }
    }

    return indices;
}

} // namespace marble

