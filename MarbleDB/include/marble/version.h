#pragma once

#include <memory>
#include <vector>
#include <atomic>
#include <string>
#include <functional>
#include <marble/status.h>
#include <marble/record.h>

namespace marble {

// Forward declarations
class TaskScheduler;
class SSTable;

// Timestamp for versioning - monotonically increasing
using Timestamp = uint64_t;

// Transaction ID for tracking transactions
using TransactionId = uint64_t;

// Version snapshot representing a point-in-time view of the database
struct VersionSnapshot {
    Timestamp timestamp;
    std::string name;  // Optional snapshot name

    VersionSnapshot(Timestamp ts = 0, const std::string& n = "")
        : timestamp(ts), name(n) {}
};

// Key with timestamp for MVCC
struct VersionedKey {
    std::shared_ptr<Key> key;
    Timestamp timestamp;
    TransactionId transaction_id;

    VersionedKey(std::shared_ptr<Key> k, Timestamp ts, TransactionId txn_id = 0)
        : key(std::move(k)), timestamp(ts), transaction_id(txn_id) {}
};

// Record with version information
struct VersionedRecord {
    std::shared_ptr<Record> record;
    Timestamp timestamp;
    TransactionId transaction_id;
    bool is_deleted = false;  // Tombstone marker

    VersionedRecord(std::shared_ptr<Record> r, Timestamp ts, TransactionId txn_id = 0, bool deleted = false)
        : record(std::move(r)), timestamp(ts), transaction_id(txn_id), is_deleted(deleted) {}
};

// Version edit operations for atomic version updates
enum class VersionEditType {
    kAddSSTable,      // Add new SSTable to a level
    kDeleteSSTable,   // Remove SSTable from a level
    kCompactRange,    // Mark range as compacted
    kUpdateTimestamp, // Update current timestamp
};

struct VersionEdit {
    VersionEditType type;
    int level = -1;  // LSM level (0-6)
    std::shared_ptr<SSTable> sstable;  // For add/delete operations
    Timestamp new_timestamp = 0;  // For timestamp updates
    std::string compaction_range;  // For compaction tracking

    VersionEdit(VersionEditType t) : type(t) {}
};

// Version represents the current state of the LSM tree
class Version {
public:
    virtual ~Version() = default;

    // Get the timestamp when this version was created
    virtual Timestamp GetTimestamp() const = 0;

    // Get SSTables for a specific level
    virtual const std::vector<std::shared_ptr<SSTable>>& GetSSTables(int level) const = 0;

    // Get all levels (for internal use)
    virtual const std::array<std::vector<std::shared_ptr<SSTable>>, 7>& GetAllLevels() const = 0;

    // Check if a key exists in this version at the given timestamp
    virtual Status Get(const Key& key, Timestamp timestamp,
                      std::shared_ptr<Record>* record) const = 0;

    // Get all versions of a key within a timestamp range
    virtual Status GetVersions(const Key& key, Timestamp start_ts, Timestamp end_ts,
                             std::vector<VersionedRecord>* records) const = 0;

    // Create a reference to this version
    virtual std::shared_ptr<Version> Reference() = 0;

    // Release a reference to this version
    virtual void Release() = 0;

    // Get reference count
    virtual size_t GetReferenceCount() const = 0;

    // Get version statistics
    virtual Status GetStats(std::string* stats) const = 0;
};

// Version set manages the current version and version history
class VersionSet {
public:
    virtual ~VersionSet() = default;

    // Get the current version (snapshot isolation)
    virtual std::shared_ptr<Version> Current() const = 0;

    // Create a new version based on current with edits applied
    virtual Status ApplyEdits(const std::vector<VersionEdit>& edits,
                            std::shared_ptr<Version>* new_version) = 0;

    // Create a snapshot at current timestamp
    virtual std::shared_ptr<Version> CreateSnapshot(Timestamp timestamp = 0) = 0;

    // Get version at specific timestamp (for time travel queries)
    virtual std::shared_ptr<Version> GetVersionAt(Timestamp timestamp) const = 0;

    // Clean up old versions (garbage collection)
    virtual Status CleanupOldVersions(Timestamp before_timestamp) = 0;

    // Get current timestamp
    virtual Timestamp CurrentTimestamp() const = 0;

    // Increment and get next timestamp
    virtual Timestamp NextTimestamp() = 0;

    // Get version set statistics
    virtual Status GetStats(std::string* stats) const = 0;
};

// Transaction timestamp provider interface
class TransactionTimestampProvider {
public:
    virtual ~TransactionTimestampProvider() = default;

    // Get current timestamp for read operations
    virtual Timestamp GetReadTimestamp() const = 0;

    // Get timestamp for write operations (higher than all previous)
    virtual Timestamp GetWriteTimestamp() = 0;

    // Create a snapshot timestamp
    virtual Timestamp CreateSnapshotTimestamp() = 0;
};

// MVCC Manager coordinates multi-version concurrency control
class MVCCManager {
public:
    explicit MVCCManager(std::unique_ptr<TransactionTimestampProvider> timestamp_provider);
    virtual ~MVCCManager() = default;

    // Begin a new transaction with snapshot isolation
    virtual Status BeginTransaction(TransactionId* transaction_id) = 0;

    // Commit a transaction
    virtual Status CommitTransaction(TransactionId transaction_id) = 0;

    // Rollback a transaction
    virtual Status RollbackTransaction(TransactionId transaction_id) = 0;

    // Check for write-write conflicts
    virtual Status CheckWriteConflict(const VersionedKey& key,
                                    TransactionId current_transaction) = 0;

    // Get the current read timestamp
    virtual Timestamp GetReadTimestamp() const = 0;

    // Get visible records for a transaction at its read timestamp
    virtual Status GetVisibleRecords(TransactionId transaction_id,
                                   const Key& key,
                                   std::vector<VersionedRecord>* records) const = 0;

    // Add a write operation to a transaction
    virtual Status AddWriteOperation(TransactionId transaction_id,
                                   const VersionedKey& key,
                                   const VersionedRecord& record) = 0;

    // Get active transaction count
    virtual size_t GetActiveTransactionCount() const = 0;

    // Get MVCC statistics
    virtual Status GetStats(std::string* stats) const = 0;

protected:
    std::unique_ptr<TransactionTimestampProvider> timestamp_provider_;
};

// Version manager factory functions
std::unique_ptr<VersionSet> CreateVersionSet();
std::unique_ptr<MVCCManager> CreateMVCCManager(
    std::unique_ptr<TransactionTimestampProvider> timestamp_provider);
std::unique_ptr<TransactionTimestampProvider> CreateDefaultTimestampProvider();

// Snapshot management
class SnapshotManager {
public:
    virtual ~SnapshotManager() = default;

    // Create a named snapshot
    virtual Status CreateSnapshot(const std::string& name,
                                std::shared_ptr<Version>* snapshot) = 0;

    // Get a snapshot by name
    virtual Status GetSnapshot(const std::string& name,
                             std::shared_ptr<Version>* snapshot) const = 0;

    // List all snapshots
    virtual Status ListSnapshots(std::vector<std::string>* snapshots) const = 0;

    // Delete a snapshot
    virtual Status DeleteSnapshot(const std::string& name) = 0;

    // Clean up unreferenced snapshots
    virtual Status CleanupSnapshots() = 0;
};

std::unique_ptr<SnapshotManager> CreateSnapshotManager(std::shared_ptr<VersionSet> version_set);

// Version garbage collector for cleaning up old versions
class VersionGarbageCollector {
public:
    virtual ~VersionGarbageCollector() = default;

    // Schedule garbage collection
    virtual Status ScheduleGC(Timestamp retention_timestamp) = 0;

    // Run garbage collection synchronously
    virtual Status RunGC() = 0;

    // Get GC statistics
    virtual Status GetStats(std::string* stats) const = 0;

    // Check if GC is currently running
    virtual bool IsGCRunning() const = 0;
};

std::unique_ptr<VersionGarbageCollector> CreateVersionGarbageCollector(
    std::shared_ptr<VersionSet> version_set, TaskScheduler* scheduler);

} // namespace marble
