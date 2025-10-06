#pragma once

#include <memory>
#include <vector>
#include <string>
#include <functional>
#include <atomic>
#include <marble/status.h>
#include <marble/file_system.h>
#include <marble/record.h>

namespace marble {

// Forward declarations
class TaskScheduler;

// WAL entry types for handling large records that span multiple log entries
enum class WalEntryType {
    kFull,      // Complete record in single entry
    kFirst,     // First part of multi-part record
    kMiddle,    // Middle part of multi-part record
    kLast,      // Last part of multi-part record
    kDelete,    // Deletion marker
    kCommit,    // Transaction commit marker
    kAbort      // Transaction abort marker
};

// WAL configuration options
struct WalOptions {
    // File system path for WAL files
    std::string wal_path = "/tmp/marble_wal";

    // Maximum WAL file size before rotation
    size_t max_file_size = 64 * 1024 * 1024;  // 64MB

    // Buffer size for WAL writes
    size_t buffer_size = 64 * 1024;  // 64KB

    // Maximum number of WAL files to keep
    size_t max_files = 10;

    // Sync mode
    enum class SyncMode {
        kAsync,     // No fsync, fastest but less durable
        kSync,      // fsync after each write, slowest but most durable
        kBatch      // fsync periodically, balanced performance/durability
    } sync_mode = SyncMode::kBatch;

    // Batch sync interval (for kBatch mode)
    size_t sync_interval = 1000;  // Sync every 1000 entries

    // Compression for WAL entries
    bool enable_compression = false;

    // Checksum verification
    bool enable_checksum = true;
};

// WAL entry containing the actual log data
struct WalEntry {
    uint64_t sequence_number = 0;
    uint64_t transaction_id = 0;
    WalEntryType entry_type = WalEntryType::kFull;
    std::shared_ptr<Key> key;
    std::shared_ptr<Record> value;  // nullptr for deletes
    uint64_t timestamp = 0;

    // For multi-part entries
    size_t part_index = 0;
    size_t total_parts = 1;

    // Checksum for data integrity
    uint32_t checksum = 0;

    WalEntry() = default;
    WalEntry(uint64_t seq, uint64_t txn_id, WalEntryType type,
             std::shared_ptr<Key> k, std::shared_ptr<Record> v,
             uint64_t ts = 0);
};

// WAL file interface for managing individual WAL files
class WalFile {
public:
    virtual ~WalFile() = default;

    // Write an entry to the WAL file
    virtual Status WriteEntry(const WalEntry& entry) = 0;

    // Read entries from the WAL file
    virtual Status ReadEntries(std::vector<WalEntry>* entries,
                             uint64_t start_sequence = 0,
                             size_t max_entries = 0) = 0;

    // Flush pending writes to disk
    virtual Status Flush() = 0;

    // Close the WAL file
    virtual Status Close() = 0;

    // Get file metadata
    virtual std::string GetFilePath() const = 0;
    virtual uint64_t GetFileId() const = 0;
    virtual size_t GetFileSize() const = 0;
    virtual uint64_t GetFirstSequence() const = 0;
    virtual uint64_t GetLastSequence() const = 0;

    // Check if file is full
    virtual bool IsFull() const = 0;
};

// Main WAL manager interface
class WalManager {
public:
    virtual ~WalManager() = default;

    // Initialize WAL manager
    virtual Status Open(const WalOptions& options) = 0;

    // Close WAL manager and flush all pending writes
    virtual Status Close() = 0;

    // Write an entry to the current WAL file
    virtual Status WriteEntry(const WalEntry& entry) = 0;

    // Write multiple entries as a batch
    virtual Status WriteBatch(const std::vector<WalEntry>& entries) = 0;

    // Get the current WAL sequence number
    virtual uint64_t GetCurrentSequence() const = 0;

    // Force sync of all pending writes
    virtual Status Sync() = 0;

    // Rotate to a new WAL file
    virtual Status Rotate() = 0;

    // Recover entries from WAL files
    virtual Status Recover(std::function<Status(const WalEntry&)> callback,
                          uint64_t start_sequence = 0) = 0;

    // Truncate WAL up to a specific sequence number
    virtual Status Truncate(uint64_t sequence_number) = 0;

    // Get WAL statistics
    virtual Status GetStats(std::string* stats) const = 0;

    // Get list of WAL files
    virtual Status ListFiles(std::vector<std::string>* files) const = 0;
};

// WAL recovery callback interface
class WalRecoveryCallback {
public:
    virtual ~WalRecoveryCallback() = default;

    // Called for each recovered WAL entry
    virtual Status OnWalEntry(const WalEntry& entry) = 0;

    // Called when recovery is complete
    virtual Status OnRecoveryComplete(uint64_t last_sequence) = 0;
};

// Factory functions
std::unique_ptr<WalManager> CreateWalManager(TaskScheduler* scheduler = nullptr);
std::unique_ptr<WalFile> CreateWalFile(const std::string& path, uint64_t file_id);

// Utility functions
uint32_t ComputeChecksum(const WalEntry& entry);
bool ValidateChecksum(const WalEntry& entry);

} // namespace marble
