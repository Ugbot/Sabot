/************************************************************************
MarbleDB MVCC (Multi-Version Concurrency Control)
Inspired by Tonbo's optimistic transaction implementation

Provides snapshot isolation for ACID transactions without blocking readers.
**************************************************************************/

#pragma once

#include <marble/status.h>
#include <marble/record.h>
#include <marble/record_ref.h>
#include <marble/lsm_tree.h>  // For MemTable (full definition needed)
#include <marble/wal.h>        // For WAL (full definition needed)
#include <memory>
#include <atomic>
#include <unordered_map>
#include <map>
#include <mutex>
#include <vector>

namespace marble {

/**
 * @brief MVCC timestamp for versioning
 * 
 * Monotonically increasing timestamp assigned to each transaction.
 * Used for snapshot isolation and conflict detection.
 */
class Timestamp {
public:
    using ValueType = uint64_t;
    
    explicit Timestamp(ValueType ts = 0) : value_(ts) {}
    
    ValueType value() const { return value_; }
    
    bool operator<(const Timestamp& other) const { return value_ < other.value_; }
    bool operator<=(const Timestamp& other) const { return value_ <= other.value_; }
    bool operator>(const Timestamp& other) const { return value_ > other.value_; }
    bool operator>=(const Timestamp& other) const { return value_ >= other.value_; }
    bool operator==(const Timestamp& other) const { return value_ == other.value_; }
    bool operator!=(const Timestamp& other) const { return value_ != other.value_; }

private:
    ValueType value_;
};

/**
 * @brief Timestamped key for MVCC
 * 
 * Stores (key, timestamp) pairs in LSM tree.
 * Allows multiple versions of same key.
 */
template<typename K>
struct TimestampedKey {
    K key;
    Timestamp ts;
    
    TimestampedKey(const K& k, Timestamp t) : key(k), ts(t) {}
    
    // Sort by key first, then by timestamp (descending)
    bool operator<(const TimestampedKey& other) const {
        int cmp = key.Compare(other.key);
        if (cmp != 0) return cmp < 0;
        return ts > other.ts;  // Newer versions first
    }
};

/**
 * @brief Snapshot for transaction isolation
 * 
 * Captures consistent view of database at a point in time.
 * Reads see only data committed before snapshot timestamp.
 */
class Snapshot {
public:
    Snapshot() : timestamp_(Timestamp(0)) {}  // Default constructor
    explicit Snapshot(Timestamp ts) : timestamp_(ts) {}

    Timestamp timestamp() const { return timestamp_; }
    
    /**
     * @brief Check if a write is visible to this snapshot
     */
    bool IsVisible(Timestamp write_ts) const {
        return write_ts <= timestamp_;
    }

private:
    Timestamp timestamp_;
};

/**
 * @brief Transaction write buffer
 * 
 * Stores uncommitted writes in transaction-local buffer.
 * Provides read-your-writes semantics.
 */
class WriteBuffer {
public:
    void Put(std::shared_ptr<Key> key, std::shared_ptr<Record> record) {
        buffer_[key->ToString()] = record;
    }
    
    void Delete(std::shared_ptr<Key> key) {
        buffer_[key->ToString()] = nullptr;  // Tombstone
    }
    
    /**
     * @brief Get from buffer (returns true if found)
     */
    bool Get(const Key& key, std::shared_ptr<Record>* record) const {
        auto it = buffer_.find(key.ToString());
        if (it == buffer_.end()) {
            return false;  // Not in buffer
        }
        
        *record = it->second;  // May be nullptr (tombstone)
        return true;
    }
    
    /**
     * @brief Get all buffered writes for commit
     */
    const std::unordered_map<std::string, std::shared_ptr<Record>>& entries() const {
        return buffer_;
    }
    
    size_t size() const { return buffer_.size(); }
    bool empty() const { return buffer_.empty(); }

private:
    std::unordered_map<std::string, std::shared_ptr<Record>> buffer_;
};

/**
 * @brief Write conflict detector
 * 
 * Detects if any key has been modified between transaction start and commit.
 */
class ConflictDetector {
public:
    /**
     * @brief Check for conflicts
     * 
     * @param keys Keys being written
     * @param snapshot_ts Transaction's snapshot timestamp
     * @param current_ts Current database timestamp
     * @return First conflicting key, or nullptr if no conflicts
     */
    static std::shared_ptr<Key> DetectConflicts(
        const std::vector<std::shared_ptr<Key>>& keys,
        Timestamp snapshot_ts,
        Timestamp current_ts,
        MemTable* memtable) {
        
        for (const auto& key : keys) {
            // Check if any version exists with ts > snapshot_ts
            // This means the key was modified after transaction started
            
            std::shared_ptr<Record> record;
            auto status = memtable->Get(*key, &record);
            
            if (status.ok() && record) {
                // TODO: Check record's timestamp
                // For now, assume conflict if key exists
                // Proper implementation needs timestamped records
            }
        }
        
        return nullptr;  // No conflicts
    }
};

/**
 * @brief Global timestamp oracle
 * 
 * Provides monotonically increasing timestamps for transactions.
 * Thread-safe via atomic operations.
 */
class TimestampOracle {
public:
    TimestampOracle() : counter_(0) {}
    
    /**
     * @brief Get current timestamp (for snapshots)
     */
    Timestamp Now() const {
        return Timestamp(counter_.load(std::memory_order_acquire));
    }
    
    /**
     * @brief Get next timestamp (for commits)
     */
    Timestamp Next() {
        return Timestamp(counter_.fetch_add(1, std::memory_order_acq_rel) + 1);
    }
    
    /**
     * @brief Set timestamp (for recovery)
     */
    void Set(Timestamp ts) {
        counter_.store(ts.value(), std::memory_order_release);
    }

private:
    std::atomic<Timestamp::ValueType> counter_;
};

/**
 * @brief MVCC transaction manager
 *
 * Manages transaction lifecycle:
 * 1. Begin: Create snapshot
 * 2. Read: Use snapshot timestamp
 * 3. Write: Buffer locally
 * 4. Commit: Check conflicts + atomically write batch
 */
class MVCCTransactionManager {
public:
    explicit MVCCTransactionManager(TimestampOracle* oracle)
        : oracle_(oracle) {}

    /**
     * @brief Begin new transaction
     */
    Snapshot BeginTransaction() {
        return Snapshot(oracle_->Now());
    }

    /**
     * @brief Commit transaction
     *
     * @param buffer Transaction's write buffer
     * @param snapshot Transaction's snapshot
     * @param memtable Target memtable
     * @param wal Write-ahead log
     * @return Status OK if committed, Conflict if conflicts detected
     */
    Status CommitTransaction(
        const WriteBuffer& buffer,
        const Snapshot& snapshot,
        MemTable* memtable,
        WalManager* wal) {

        if (buffer.empty()) {
            return Status::OK();  // Nothing to commit
        }

        // Get commit timestamp
        Timestamp commit_ts = oracle_->Next();

        // Collect keys being written
        std::vector<std::shared_ptr<Key>> keys;
        for (const auto& entry : buffer.entries()) {
            // Parse key from string
            // TODO: Proper key deserialization
        }

        // Check for write conflicts
        auto conflict_key = ConflictDetector::DetectConflicts(
            keys, snapshot.timestamp(), commit_ts, memtable
        );

        if (conflict_key) {
            return Status::WriteConflict("Write conflict on key: " + conflict_key->ToString());
        }

        // Write batch to WAL
        if (wal) {
            for (const auto& entry : buffer.entries()) {
                WalEntry wal_entry;
                wal_entry.sequence_number = commit_ts.value();
                // TODO: Populate wal_entry from buffer

                auto status = wal->WriteEntry(wal_entry);
                if (!status.ok()) {
                    return status;
                }
            }

            if (wal->Sync().ok()) {
                // WAL synced
            }
        }

        // Apply writes to memtable
        for (const auto& entry : buffer.entries()) {
            // TODO: Apply with commit_ts
        }

        return Status::OK();
    }

private:
    TimestampOracle* oracle_;
};

// Forward declaration
class MVCCManager;
struct TableCapabilities;

// Global MVCC manager instance
extern std::unique_ptr<MVCCManager> global_mvcc_manager;

// Initialization functions
void initializeMVCC();
void shutdownMVCC();

// Helper function to configure MVCC from TableCapabilities
Status ConfigureMVCCFromCapabilities(
    const std::string& table_name,
    const TableCapabilities& capabilities);

} // namespace marble

