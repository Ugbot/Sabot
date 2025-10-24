#include "marble/mvcc.h"
#include "marble/table_capabilities.h"
#include "marble/column_family.h"
#include <algorithm>
#include <chrono>

namespace marble {

/**
 * @brief MVCC Manager - Central coordinator for multi-version concurrency control
 *
 * Provides:
 * - Transaction lifecycle management
 * - Snapshot isolation
 * - Conflict detection
 * - Garbage collection of old versions
 */
class MVCCManager {
public:
    MVCCManager() : oracle_(), next_txn_id_(1) {
        // Initialize timestamp oracle
        oracle_.Set(Timestamp(1000));  // Start at 1000 to leave room for reserved timestamps
    }

    /**
     * @brief Begin a new transaction
     *
     * @param read_only Whether this is a read-only transaction
     * @return Transaction ID and snapshot
     */
    struct TransactionContext {
        uint64_t txn_id;
        Snapshot snapshot;
        WriteBuffer write_buffer;
        bool read_only;
        Timestamp start_time;
    };

    TransactionContext BeginTransaction(bool read_only = false) {
        std::lock_guard<std::mutex> lock(mutex_);

        TransactionContext ctx;
        ctx.txn_id = next_txn_id_++;
        ctx.snapshot = Snapshot(oracle_.Now());
        ctx.read_only = read_only;
        ctx.start_time = oracle_.Now();

        // Register active transaction
        active_transactions_[ctx.txn_id] = ctx.snapshot.timestamp();

        // Track snapshot for GC
        active_snapshots_[ctx.txn_id] = ctx.snapshot.timestamp();

        // Log transaction start
        if (global_logger) {
            global_logger->debug("MVCCManager",
                "Started transaction " + std::to_string(ctx.txn_id) +
                " at timestamp " + std::to_string(ctx.snapshot.timestamp().value()));
        }

        // Update metrics
        if (global_metrics_collector) {
            global_metrics_collector->incrementCounter("marble.mvcc.transactions_started");
            if (read_only) {
                global_metrics_collector->incrementCounter("marble.mvcc.readonly_transactions");
            }
        }

        return ctx;
    }

    /**
     * @brief Get read timestamp for transaction
     */
    Timestamp GetReadTimestamp(uint64_t txn_id) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = active_transactions_.find(txn_id);
        if (it != active_transactions_.end()) {
            return it->second;
        }

        // Transaction not found, return current timestamp
        return oracle_.Now();
    }

    /**
     * @brief Commit a transaction
     *
     * @param ctx Transaction context
     * @return Status OK if committed, Conflict if conflicts detected
     */
    Status CommitTransaction(TransactionContext& ctx) {
        if (ctx.read_only) {
            // Read-only transactions don't write anything
            std::lock_guard<std::mutex> lock(mutex_);
            active_transactions_.erase(ctx.txn_id);
            active_snapshots_.erase(ctx.txn_id);

            if (global_logger) {
                global_logger->debug("MVCCManager",
                    "Committed read-only transaction " + std::to_string(ctx.txn_id));
            }

            return Status::OK();
        }

        if (ctx.write_buffer.empty()) {
            // No writes to commit
            std::lock_guard<std::mutex> lock(mutex_);
            active_transactions_.erase(ctx.txn_id);
            active_snapshots_.erase(ctx.txn_id);
            return Status::OK();
        }

        // Get commit timestamp (must be unique and > all snapshot timestamps)
        Timestamp commit_ts = oracle_.Next();

        // Check for write-write conflicts
        {
            std::lock_guard<std::mutex> lock(mutex_);

            for (const auto& entry : ctx.write_buffer.entries()) {
                const auto& key_str = entry.first;

                // Check if this key was modified by another transaction
                // that committed after our snapshot
                auto version_it = key_versions_.find(key_str);
                if (version_it != key_versions_.end()) {
                    const auto& versions = version_it->second;

                    // Check if any version exists with ts > snapshot_ts
                    for (const auto& version : versions) {
                        if (version.commit_ts > ctx.snapshot.timestamp()) {
                            // Conflict detected!
                            active_transactions_.erase(ctx.txn_id);
                            active_snapshots_.erase(ctx.txn_id);

                            if (global_logger) {
                                global_logger->warn("MVCCManager",
                                    "Write conflict on key " + key_str +
                                    " for transaction " + std::to_string(ctx.txn_id));
                            }

                            if (global_metrics_collector) {
                                global_metrics_collector->incrementCounter("marble.mvcc.conflicts");
                            }

                            return Status::WriteConflict("Write conflict on key: " + key_str);
                        }
                    }
                }
            }

            // No conflicts - commit all writes
            for (const auto& entry : ctx.write_buffer.entries()) {
                const auto& key_str = entry.first;
                const auto& record = entry.second;

                VersionInfo version;
                version.commit_ts = commit_ts;
                version.record = record;

                key_versions_[key_str].push_back(version);

                // Sort versions by timestamp (newest first)
                std::sort(key_versions_[key_str].begin(), key_versions_[key_str].end(),
                    [](const VersionInfo& a, const VersionInfo& b) {
                        return a.commit_ts > b.commit_ts;
                    });
            }

            // Remove transaction from active set
            active_transactions_.erase(ctx.txn_id);
            active_snapshots_.erase(ctx.txn_id);

            // Update stats
            stats_.total_commits++;
            stats_.total_writes += ctx.write_buffer.size();

            if (global_logger) {
                global_logger->info("MVCCManager",
                    "Committed transaction " + std::to_string(ctx.txn_id) +
                    " with " + std::to_string(ctx.write_buffer.size()) + " writes" +
                    " at timestamp " + std::to_string(commit_ts.value()));
            }

            if (global_metrics_collector) {
                global_metrics_collector->incrementCounter("marble.mvcc.transactions_committed");
                global_metrics_collector->incrementCounter("marble.mvcc.writes_committed",
                    ctx.write_buffer.size());
            }
        }

        return Status::OK();
    }

    /**
     * @brief Rollback a transaction
     */
    Status RollbackTransaction(TransactionContext& ctx) {
        std::lock_guard<std::mutex> lock(mutex_);

        active_transactions_.erase(ctx.txn_id);
        active_snapshots_.erase(ctx.txn_id);

        stats_.total_rollbacks++;

        if (global_logger) {
            global_logger->info("MVCCManager",
                "Rolled back transaction " + std::to_string(ctx.txn_id));
        }

        if (global_metrics_collector) {
            global_metrics_collector->incrementCounter("marble.mvcc.transactions_rolled_back");
        }

        return Status::OK();
    }

    /**
     * @brief Get a record for a given snapshot
     *
     * @param key Key to retrieve
     * @param snapshot Snapshot timestamp
     * @param record Output record
     * @return Status OK if found, NotFound if no visible version
     */
    Status GetForSnapshot(const Key& key, const Snapshot& snapshot, std::shared_ptr<Record>* record) {
        std::lock_guard<std::mutex> lock(mutex_);

        std::string key_str = key.ToString();
        auto it = key_versions_.find(key_str);

        if (it == key_versions_.end()) {
            return Status::NotFound("Key not found: " + key_str);
        }

        const auto& versions = it->second;

        // Find first version visible to snapshot (versions sorted newest first)
        for (const auto& version : versions) {
            if (snapshot.IsVisible(version.commit_ts)) {
                if (version.record == nullptr) {
                    // Tombstone - key was deleted
                    return Status::NotFound("Key deleted: " + key_str);
                }

                *record = version.record;
                return Status::OK();
            }
        }

        return Status::NotFound("No visible version for key: " + key_str);
    }

    /**
     * @brief Run garbage collection to remove old versions
     *
     * @param policy GC policy from TableCapabilities
     * @param max_versions_per_key Maximum versions to keep per key
     * @param gc_timestamp_ms Timestamp threshold for GC
     * @return Number of versions removed
     */
    uint64_t GarbageCollect(
        TableCapabilities::MVCCSettings::GCPolicy policy,
        size_t max_versions_per_key,
        uint64_t gc_timestamp_ms) {

        std::lock_guard<std::mutex> lock(mutex_);

        uint64_t versions_removed = 0;

        // Find minimum active snapshot timestamp
        Timestamp min_snapshot_ts = oracle_.Now();
        if (!active_snapshots_.empty()) {
            for (const auto& entry : active_snapshots_) {
                if (entry.second < min_snapshot_ts) {
                    min_snapshot_ts = entry.second;
                }
            }
        }

        // GC each key's versions
        for (auto& entry : key_versions_) {
            auto& versions = entry.second;

            if (versions.empty()) continue;

            std::vector<VersionInfo> kept_versions;

            switch (policy) {
                case TableCapabilities::MVCCSettings::GCPolicy::kKeepAllVersions:
                    // Keep all versions
                    kept_versions = versions;
                    break;

                case TableCapabilities::MVCCSettings::GCPolicy::kKeepRecentVersions:
                    // Keep up to max_versions_per_key most recent versions
                    for (size_t i = 0; i < versions.size() && i < max_versions_per_key; ++i) {
                        kept_versions.push_back(versions[i]);
                    }
                    versions_removed += (versions.size() > max_versions_per_key) ?
                        (versions.size() - max_versions_per_key) : 0;
                    break;

                case TableCapabilities::MVCCSettings::GCPolicy::kKeepVersionsUntil:
                    // Keep versions committed after gc_timestamp_ms
                    for (const auto& version : versions) {
                        if (version.commit_ts.value() > gc_timestamp_ms) {
                            kept_versions.push_back(version);
                        } else {
                            versions_removed++;
                        }
                    }
                    break;
            }

            // Also remove versions not visible to any active snapshot
            // (except the most recent version)
            std::vector<VersionInfo> final_versions;
            for (size_t i = 0; i < kept_versions.size(); ++i) {
                if (i == 0) {
                    // Always keep most recent version
                    final_versions.push_back(kept_versions[i]);
                } else if (kept_versions[i].commit_ts >= min_snapshot_ts) {
                    // Keep if visible to active snapshots
                    final_versions.push_back(kept_versions[i]);
                } else {
                    versions_removed++;
                }
            }

            versions = final_versions;
        }

        stats_.total_gc_runs++;
        stats_.total_versions_removed += versions_removed;

        if (global_logger) {
            global_logger->info("MVCCManager",
                "Garbage collection removed " + std::to_string(versions_removed) + " versions");
        }

        if (global_metrics_collector) {
            global_metrics_collector->incrementCounter("marble.mvcc.gc_runs");
            global_metrics_collector->incrementCounter("marble.mvcc.versions_removed", versions_removed);
        }

        return versions_removed;
    }

    /**
     * @brief Get statistics
     */
    struct MVCCStats {
        uint64_t total_commits = 0;
        uint64_t total_rollbacks = 0;
        uint64_t total_writes = 0;
        uint64_t total_gc_runs = 0;
        uint64_t total_versions_removed = 0;
        size_t active_transactions_count = 0;
        size_t active_snapshots_count = 0;
        size_t total_keys = 0;
        size_t total_versions = 0;
    };

    MVCCStats GetStats() const {
        std::lock_guard<std::mutex> lock(mutex_);

        MVCCStats stats = stats_;
        stats.active_transactions_count = active_transactions_.size();
        stats.active_snapshots_count = active_snapshots_.size();
        stats.total_keys = key_versions_.size();

        size_t total_versions = 0;
        for (const auto& entry : key_versions_) {
            total_versions += entry.second.size();
        }
        stats.total_versions = total_versions;

        return stats;
    }

    /**
     * @brief Get current timestamp
     */
    Timestamp Now() const {
        return oracle_.Now();
    }

private:
    struct VersionInfo {
        Timestamp commit_ts;
        std::shared_ptr<Record> record;  // nullptr = tombstone (deleted)
    };

    TimestampOracle oracle_;
    std::atomic<uint64_t> next_txn_id_;

    // Active transactions: txn_id -> snapshot_ts
    std::unordered_map<uint64_t, Timestamp> active_transactions_;

    // Active snapshots: txn_id -> snapshot_ts (for GC)
    std::unordered_map<uint64_t, Timestamp> active_snapshots_;

    // Version store: key -> list of versions (sorted newest first)
    std::unordered_map<std::string, std::vector<VersionInfo>> key_versions_;

    mutable std::mutex mutex_;
    MVCCStats stats_;
};

// Global MVCC manager instance
std::unique_ptr<MVCCManager> global_mvcc_manager;

void initializeMVCC() {
    global_mvcc_manager = std::make_unique<MVCCManager>();
}

void shutdownMVCC() {
    global_mvcc_manager.reset();
}

// Helper function to convert TableCapabilities::MVCCSettings to MVCC configuration
Status ConfigureMVCCFromCapabilities(
    const std::string& table_name,
    const TableCapabilities& capabilities) {

    if (!capabilities.enable_mvcc) {
        if (global_logger) {
            global_logger->debug("MVCCManager", "MVCC not enabled for table: " + table_name);
        }
        return Status::OK();  // Not an error, just not enabled
    }

    // MVCC is enabled - log configuration
    if (global_logger) {
        global_logger->info("MVCCManager",
            "MVCC enabled for table " + table_name +
            " with max_versions=" + std::to_string(capabilities.mvcc_settings.max_versions_per_key) +
            ", gc_policy=" + std::to_string(static_cast<int>(capabilities.mvcc_settings.gc_policy)));
    }

    // MVCC configuration is table-specific, stored in capabilities
    // No global registration needed - checked at runtime via cf->GetCapabilities().enable_mvcc

    return Status::OK();
}

} // namespace marble
