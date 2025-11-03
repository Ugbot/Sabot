#include <marble/version.h>
#include <marble/task_scheduler.h>
#include <algorithm>
#include <chrono>
#include <sstream>
#include <iostream>
#include <unordered_map>
#include <deque>

namespace marble {

// Default timestamp provider implementation
class DefaultTimestampProvider : public TransactionTimestampProvider {
public:
    DefaultTimestampProvider() : current_timestamp_(1) {}  // Start at 1, 0 is invalid

    Timestamp GetReadTimestamp() const override {
        return Timestamp(current_timestamp_.load(std::memory_order_acquire));
    }

    Timestamp GetWriteTimestamp() override {
        uint64_t ts = current_timestamp_.fetch_add(1, std::memory_order_acq_rel);
        return Timestamp(ts + 1);
    }

    Timestamp CreateSnapshotTimestamp() override {
        return Timestamp(current_timestamp_.load(std::memory_order_acquire));
    }

private:
    std::atomic<uint64_t> current_timestamp_;
};

// Version implementation
class VersionImpl : public Version, public std::enable_shared_from_this<VersionImpl> {
public:
    VersionImpl(Timestamp timestamp, const std::array<std::vector<std::shared_ptr<SSTable>>, 7>& levels)
        : timestamp_(timestamp), levels_(levels), reference_count_(1) {}

    Timestamp GetTimestamp() const override {
        return timestamp_;
    }

    const std::vector<std::shared_ptr<SSTable>>& GetSSTables(int level) const override {
        static const std::vector<std::shared_ptr<SSTable>> empty_vector;
        if (level < 0 || level >= 7) {
            return empty_vector;
        }
        return levels_[level];
    }

    Status Get(const Key& key, Timestamp timestamp,
              std::shared_ptr<Record>* record) const override {
        // Simplified implementation for demo - in practice, this would query SSTables
        // For now, return NotFound since we don't have actual SSTable data
        return Status::NotFound("Key not found in version (SSTable querying not implemented)");
    }

    Status GetVersions(const Key& key, Timestamp start_ts, Timestamp end_ts,
                     std::vector<VersionedRecord>* records) const override {
        // Simplified implementation for demo - in practice, this would search
        // through all SSTables and reconstruct version chains
        records->clear();

        // For demo purposes, return empty list
        return Status::OK();
    }

    std::shared_ptr<Version> Reference() override {
        reference_count_.fetch_add(1, std::memory_order_acq_rel);
        return shared_from_this();
    }

    void Release() override {
        if (reference_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // Last reference, object will be destroyed
        }
    }

    size_t GetReferenceCount() const override {
        return reference_count_.load(std::memory_order_acquire);
    }

    Status GetStats(std::string* stats) const override {
        std::stringstream ss;
        ss << "Version{timestamp=" << timestamp_.value()
           << ", ref_count=" << reference_count_.load();

        for (int level = 0; level < 7; ++level) {
            ss << ", level" << level << "_files=" << levels_[level].size();
        }
        ss << "}";

        *stats = ss.str();
        return Status::OK();
    }

    const std::array<std::vector<std::shared_ptr<SSTable>>, 7>& GetAllLevels() const override {
        return levels_;
    }

private:
    Timestamp timestamp_;
    std::array<std::vector<std::shared_ptr<SSTable>>, 7> levels_;
    std::atomic<size_t> reference_count_;
};

// VersionSet implementation
class VersionSetImpl : public VersionSet {
public:
    VersionSetImpl() : current_timestamp_(std::make_unique<DefaultTimestampProvider>()) {
        // Create initial empty version
        std::array<std::vector<std::shared_ptr<SSTable>>, 7> empty_levels{};
        current_version_ = std::make_shared<VersionImpl>(Timestamp(0), empty_levels);
    }

    std::shared_ptr<Version> Current() const override {
        std::unique_lock<std::mutex> lock(mutex_);
        return current_version_;
    }

    Status ApplyEdits(const std::vector<VersionEdit>& edits,
                    std::shared_ptr<Version>* new_version) override {
        std::unique_lock<std::mutex> lock(mutex_);

        // Create a copy of current levels
        auto new_levels = current_version_->GetAllLevels();

        // Apply edits
        for (const auto& edit : edits) {
            switch (edit.type) {
                case VersionEditType::kAddSSTable:
                    if (edit.level >= 0 && edit.level < 7 && edit.sstable) {
                        new_levels[edit.level].push_back(edit.sstable);
                    }
                    break;

                case VersionEditType::kDeleteSSTable:
                    if (edit.level >= 0 && edit.level < 7 && edit.sstable) {
                        auto& level_files = new_levels[edit.level];
                        auto it = std::find(level_files.begin(), level_files.end(), edit.sstable);
                        if (it != level_files.end()) {
                            level_files.erase(it);
                        }
                    }
                    break;

                case VersionEditType::kCompactRange:
                    // Mark compaction range (simplified - no actual compaction tracking)
                    break;

                case VersionEditType::kUpdateTimestamp:
                    // Timestamp updates handled separately
                    break;
            }
        }

        // Create new version
        Timestamp new_timestamp = current_timestamp_->GetWriteTimestamp();
        *new_version = std::make_shared<VersionImpl>(new_timestamp, new_levels);

        // Update current version
        current_version_ = *new_version;

        // Keep history for time travel queries (simplified)
        version_history_.push_back(current_version_);
        if (version_history_.size() > 100) {  // Limit history size
            version_history_.pop_front();
        }

        return Status::OK();
    }

    std::shared_ptr<Version> CreateSnapshot(Timestamp timestamp) override {
        std::unique_lock<std::mutex> lock(mutex_);
        if (timestamp == Timestamp(0)) {
            timestamp = current_timestamp_->CreateSnapshotTimestamp();
        }
        return std::make_shared<VersionImpl>(timestamp,
            current_version_->GetAllLevels());
    }

    std::shared_ptr<Version> GetVersionAt(Timestamp timestamp) const override {
        std::unique_lock<std::mutex> lock(mutex_);

        // Find the version with the largest timestamp <= requested timestamp
        for (auto it = version_history_.rbegin(); it != version_history_.rend(); ++it) {
            if ((*it)->GetTimestamp() <= timestamp) {
                return *it;
            }
        }

        // Fallback to current version
        return current_version_;
    }

    Status CleanupOldVersions(Timestamp before_timestamp) override {
        std::unique_lock<std::mutex> lock(mutex_);

        // Remove versions older than the specified timestamp
        auto it = version_history_.begin();
        while (it != version_history_.end()) {
            if ((*it)->GetTimestamp() < before_timestamp) {
                it = version_history_.erase(it);
            } else {
                ++it;
            }
        }

        return Status::OK();
    }

    Timestamp CurrentTimestamp() const override {
        return current_timestamp_->GetReadTimestamp();
    }

    Timestamp NextTimestamp() override {
        return current_timestamp_->GetWriteTimestamp();
    }

    Status GetStats(std::string* stats) const override {
        std::unique_lock<std::mutex> lock(mutex_);

        std::stringstream ss;
        ss << "VersionSet{current_ts=" << CurrentTimestamp().value()
           << ", history_size=" << version_history_.size()
           << ", current_version_refs=" << current_version_->GetReferenceCount()
           << "}";

        *stats = ss.str();
        return Status::OK();
    }

private:

    std::unique_ptr<DefaultTimestampProvider> current_timestamp_;
    std::shared_ptr<Version> current_version_;
    std::deque<std::shared_ptr<Version>> version_history_;  // For time travel
    mutable std::mutex mutex_;
};

// MVCC Manager implementation
MVCCManager::MVCCManager(std::unique_ptr<TransactionTimestampProvider> timestamp_provider)
    : timestamp_provider_(std::move(timestamp_provider)) {}

class MVCCManagerImpl : public MVCCManager {
public:
    MVCCManagerImpl(std::unique_ptr<TransactionTimestampProvider> timestamp_provider)
        : MVCCManager(std::move(timestamp_provider)), next_transaction_id_(1) {}

    TransactionContext BeginTransaction(bool read_only) override {
        std::unique_lock<std::mutex> lock(txn_mutex_);

        TransactionId txn_id = next_transaction_id_++;
        Timestamp read_ts = timestamp_provider_->GetReadTimestamp();

        active_transactions_[txn_id] = TransactionInfo{
            txn_id,
            read_ts,
            std::chrono::steady_clock::now()
        };

        TransactionContext context;
        context.txn_id = txn_id;
        context.snapshot = Snapshot(read_ts);
        context.write_buffer = nullptr;  // Will be set by caller
        context.read_only = read_only;
        context.start_time = read_ts;

        return context;
    }

    Status CommitTransaction(const TransactionContext& context) override {
        std::unique_lock<std::mutex> lock(txn_mutex_);

        auto it = active_transactions_.find(context.txn_id);
        if (it == active_transactions_.end()) {
            return Status::InvalidArgument("Transaction not found or already committed");
        }

        // Apply all writes from this transaction
        // In a real implementation, this would coordinate with the WAL and version manager

        active_transactions_.erase(it);
        return Status::OK();
    }

    Status RollbackTransaction(const TransactionContext& context) override {
        std::unique_lock<std::mutex> lock(txn_mutex_);

        auto it = active_transactions_.find(context.txn_id);
        if (it == active_transactions_.end()) {
            return Status::InvalidArgument("Transaction not found");
        }

        // Discard all writes from this transaction
        transaction_writes_.erase(context.txn_id);
        active_transactions_.erase(it);
        return Status::OK();
    }

    Status CheckWriteConflict(const VersionedKey& key,
                            TransactionId current_transaction) override {
        std::unique_lock<std::mutex> lock(rw_mutex_);

        // Check if any active transaction has written to the same key
        for (const auto& [txn_id, writes] : transaction_writes_) {
            if (txn_id == current_transaction) continue;  // Skip current transaction

            for (const auto& write : writes) {
                // Simple key comparison (in practice, would need proper key comparison)
                if (write.key.key->ToString() == key.key->ToString()) {
                    return Status::WriteConflict("Write-write conflict detected");
                }
            }
        }

        return Status::OK();
    }

    Timestamp GetReadTimestamp() const override {
        return timestamp_provider_->GetReadTimestamp();
    }

    Status GetVisibleRecords(TransactionId transaction_id,
                           const Key& key,
                           std::vector<VersionedRecord>* records) const override {
        std::unique_lock<std::mutex> lock(rw_mutex_);

        auto txn_it = active_transactions_.find(transaction_id);
        if (txn_it == active_transactions_.end()) {
            return Status::InvalidArgument("Transaction not found");
        }

        Timestamp read_timestamp = txn_it->second.read_timestamp;
        records->clear();

        // Get committed versions visible to this transaction
        // In a real implementation, this would query the version manager
        // For now, return empty list

        return Status::OK();
    }

    Status GetForSnapshot(const Key& key, Timestamp snapshot_ts,
                        std::shared_ptr<Record>* record) override {
        std::unique_lock<std::mutex> lock(rw_mutex_);

        // Get record visible at snapshot timestamp
        // In a real implementation, this would query the version manager
        // For now, return NotFound
        return Status::NotFound("Record not found (GetForSnapshot not fully implemented)");
    }

    Status AddWriteOperation(TransactionId transaction_id,
                           const VersionedKey& key,
                           const VersionedRecord& record) override {
        std::unique_lock<std::mutex> lock(rw_mutex_);

        auto txn_it = active_transactions_.find(transaction_id);
        if (txn_it == active_transactions_.end()) {
            return Status::InvalidArgument("Transaction not found");
        }

        // Check for write conflicts
        Status conflict_status = CheckWriteConflict(key, transaction_id);
        if (!conflict_status.ok()) {
            return conflict_status;
        }

        // Add to transaction's write set
        transaction_writes_[transaction_id].push_back(WriteOperation{key, record});

        return Status::OK();
    }

    size_t GetActiveTransactionCount() const override {
        std::unique_lock<std::mutex> lock(rw_mutex_);
        return active_transactions_.size();
    }

    Status GetStats(std::string* stats) const override {
        std::unique_lock<std::mutex> lock(rw_mutex_);

        std::stringstream ss;
        ss << "MVCCManager{active_txns=" << active_transactions_.size()
           << ", pending_writes=";

        size_t total_writes = 0;
        for (const auto& [txn_id, writes] : transaction_writes_) {
            total_writes += writes.size();
        }
        ss << total_writes << "}";

        *stats = ss.str();
        return Status::OK();
    }

private:
    struct TransactionInfo {
        TransactionId id;
        Timestamp read_timestamp;
        std::chrono::steady_clock::time_point start_time;
    };

    struct WriteOperation {
        VersionedKey key;
        VersionedRecord record;
    };

    std::atomic<TransactionId> next_transaction_id_;
    std::unordered_map<TransactionId, TransactionInfo> active_transactions_;
    std::unordered_map<TransactionId, std::vector<WriteOperation>> transaction_writes_;

    mutable std::mutex rw_mutex_;
    mutable std::mutex txn_mutex_;  // For transaction management
};

// Snapshot manager implementation
class SnapshotManagerImpl : public SnapshotManager {
public:
    explicit SnapshotManagerImpl(std::shared_ptr<VersionSet> version_set)
        : version_set_(version_set) {}

    Status CreateSnapshot(const std::string& name,
                        std::shared_ptr<Version>* snapshot) override {
        std::unique_lock<std::mutex> lock(mutex_);

        if (snapshots_.find(name) != snapshots_.end()) {
            return Status::AlreadyExists("Snapshot with this name already exists");
        }

        *snapshot = version_set_->CreateSnapshot();
        snapshots_[name] = *snapshot;

        return Status::OK();
    }

    Status GetSnapshot(const std::string& name,
                     std::shared_ptr<Version>* snapshot) const override {
        std::unique_lock<std::mutex> lock(mutex_);

        auto it = snapshots_.find(name);
        if (it == snapshots_.end()) {
            return Status::NotFound("Snapshot not found");
        }

        *snapshot = it->second;
        return Status::OK();
    }

    Status ListSnapshots(std::vector<std::string>* snapshots) const override {
        std::unique_lock<std::mutex> lock(mutex_);

        snapshots->clear();
        snapshots->reserve(snapshots_.size());
        for (const auto& [name, _] : snapshots_) {
            snapshots->push_back(name);
        }

        return Status::OK();
    }

    Status DeleteSnapshot(const std::string& name) override {
        std::unique_lock<std::mutex> lock(mutex_);

        auto it = snapshots_.find(name);
        if (it == snapshots_.end()) {
            return Status::NotFound("Snapshot not found");
        }

        snapshots_.erase(it);
        return Status::OK();
    }

    Status CleanupSnapshots() override {
        std::unique_lock<std::mutex> lock(mutex_);

        // Remove snapshots that have no external references
        auto it = snapshots_.begin();
        while (it != snapshots_.end()) {
            if (it->second->GetReferenceCount() == 1) {  // Only we hold a reference
                it = snapshots_.erase(it);
            } else {
                ++it;
            }
        }

        return Status::OK();
    }

private:
    std::shared_ptr<VersionSet> version_set_;
    std::unordered_map<std::string, std::shared_ptr<Version>> snapshots_;
    mutable std::mutex mutex_;
};

// Version garbage collector implementation
class VersionGarbageCollectorImpl : public VersionGarbageCollector {
public:
    VersionGarbageCollectorImpl(std::shared_ptr<VersionSet> version_set, TaskScheduler* scheduler)
        : version_set_(version_set), scheduler_(scheduler), gc_scheduled_(false) {}

    Status ScheduleGC(Timestamp retention_timestamp) override {
        std::unique_lock<std::mutex> lock(mutex_);
        retention_timestamp_ = retention_timestamp;
        gc_scheduled_ = true;
        return Status::OK();
    }

    Status RunGC() override {
        std::unique_lock<std::mutex> lock(mutex_);

        if (!gc_scheduled_) {
            return Status::OK();  // No GC scheduled
        }

        // Run garbage collection
        Status status = version_set_->CleanupOldVersions(retention_timestamp_);
        gc_scheduled_ = false;

        return status;
    }

    Status GetStats(std::string* stats) const override {
        std::unique_lock<std::mutex> lock(mutex_);

        std::stringstream ss;
        ss << "VersionGC{scheduled=" << (gc_scheduled_ ? "true" : "false")
           << ", retention_ts=" << retention_timestamp_.value()
           << "}";

        *stats = ss.str();
        return Status::OK();
    }

    bool IsGCRunning() const override {
        std::unique_lock<std::mutex> lock(mutex_);
        return gc_running_;
    }

private:
    std::shared_ptr<VersionSet> version_set_;
    TaskScheduler* scheduler_;
    Timestamp retention_timestamp_;
    bool gc_scheduled_;
    mutable bool gc_running_;
    mutable std::mutex mutex_;
};

// Version implementation helper - removed, now implemented in class

// Factory functions
std::unique_ptr<VersionSet> CreateVersionSet() {
    return std::make_unique<VersionSetImpl>();
}

std::unique_ptr<MVCCManager> CreateMVCCManager(
    std::unique_ptr<TransactionTimestampProvider> timestamp_provider) {
    return std::make_unique<MVCCManagerImpl>(std::move(timestamp_provider));
}

std::unique_ptr<TransactionTimestampProvider> CreateDefaultTimestampProvider() {
    return std::make_unique<DefaultTimestampProvider>();
}

std::unique_ptr<SnapshotManager> CreateSnapshotManager(std::shared_ptr<VersionSet> version_set) {
    return std::make_unique<SnapshotManagerImpl>(version_set);
}

std::unique_ptr<VersionGarbageCollector> CreateVersionGarbageCollector(
    std::shared_ptr<VersionSet> version_set, TaskScheduler* scheduler) {
    return std::make_unique<VersionGarbageCollectorImpl>(version_set, scheduler);
}

} // namespace marble
