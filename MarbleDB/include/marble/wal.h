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
class WalManagerImpl;

// Factory function to create WAL manager (declared after WalManager class)

// WAL entry types for handling large records that span multiple log entries
enum class WalEntryType {
    kPut,       // Put operation
    kDelete,    // Delete operation
    kMerge,     // Merge operation
    kFull,      // Complete record in single entry
    kFirst,     // First part of multi-part record
    kMiddle,    // Middle part of multi-part record
    kLast,      // Last part of multi-part record
    kCommit,    // Transaction commit marker
    kAbort      // Transaction abort marker
};

// WAL entry structure for simple operations
struct WalEntry {
    uint64_t sequence_number = 0;
    uint64_t transaction_id = 0;
    WalEntryType entry_type = WalEntryType::kFull;
    std::shared_ptr<Key> key;
    std::shared_ptr<Record> value;  // nullptr for deletes
    uint64_t timestamp = 0;
    uint64_t batch_id = 0;  // For batch operations

    // For multi-part entries
    size_t part_index = 0;
    size_t total_parts = 1;

    // Checksum for data integrity
    uint32_t checksum = 0;

    WalEntry() = default;
    WalEntry(uint64_t seq, uint64_t ts, WalEntryType type,
             std::shared_ptr<Key> k, std::shared_ptr<Record> v, uint64_t bid = 0)
        : sequence_number(seq), entry_type(type), key(std::move(k)),
          value(std::move(v)), timestamp(ts), batch_id(bid) {}
    WalEntry(uint64_t seq, WalEntryType type, uint64_t txn_id,
             std::shared_ptr<Key> k, std::shared_ptr<Record> v,
             uint64_t ts = 0)
        : sequence_number(seq), transaction_id(txn_id), entry_type(type),
          key(std::move(k)), value(std::move(v)), timestamp(ts) {}
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

// Memory-mapped ring buffer WAL options
struct MMRingBufferWalOptions : public WalOptions {
    // Size of the ring buffer file in bytes
    size_t buffer_size = 64 * 1024 * 1024;  // 64MB default

    // Maximum entry size (to prevent single entries from wrapping around)
    size_t max_entry_size = 1024 * 1024;   // 1MB max entry size

    // Number of entries to keep in memory for quick access
    size_t in_memory_cache_size = 1000;
};

// Memory-mapped ring buffer WAL file implementation
class MMRingBufferWalFile : public WalFile {
public:
    explicit MMRingBufferWalFile(const std::string& path,
                                const MMRingBufferWalOptions& options = MMRingBufferWalOptions());
    ~MMRingBufferWalFile() override;

    Status WriteEntry(const WalEntry& entry) override;
    Status ReadEntries(std::vector<WalEntry>* entries,
                      uint64_t start_sequence = 0,
                      size_t max_entries = 0) override;
    Status Flush() override;
    Status Close() override;

    // Public initialization (called by manager)
    Status InitializeRingBuffer();

    // File metadata
    std::string GetFilePath() const override { return path_; }
    uint64_t GetFileId() const override { return 0; }  // Single file implementation
    size_t GetFileSize() const override { return options_.buffer_size; }
    uint64_t GetFirstSequence() const override;
    uint64_t GetLastSequence() const override;
    bool IsFull() const override { return false; }  // Ring buffer is never "full"

    // Ring buffer specific methods
    Status GetRingBufferStats(std::string* stats) const;
    size_t GetWriteOffset() const { return write_offset_; }
    size_t GetReadOffset() const { return read_offset_; }

private:
    struct RingBufferHeader {
        uint64_t magic = 0x4D4D57414C;  // "MMWAL"
        uint32_t version = 1;
        uint64_t first_sequence = 0;
        uint64_t last_sequence = 0;
        size_t write_offset = sizeof(RingBufferHeader);
        size_t read_offset = sizeof(RingBufferHeader);
        uint32_t checksum = 0;
    };

    Status SerializeEntry(const WalEntry& entry, std::string* buffer);
    Status DeserializeEntry(const void* data, size_t size, WalEntry* entry);
    Status WriteData(size_t offset, const void* data, size_t size);
    Status ReadData(size_t offset, void* data, size_t size);
    size_t GetNextOffset(size_t current_offset, size_t data_size) const;
    bool WouldWrap(size_t current_offset, size_t data_size) const;
    Status AdvanceReadOffset(size_t new_offset);
    Status AdvanceWriteOffset(size_t new_offset);
    uint32_t ComputeChecksum(const void* data, size_t size) const;

    std::string path_;
    MMRingBufferWalOptions options_;
    std::unique_ptr<FileHandle> file_handle_;
    RingBufferHeader* header_;
    void* buffer_start_;
    size_t buffer_size_;
    std::atomic<size_t> write_offset_;
    std::atomic<size_t> read_offset_;
    std::mutex mutex_;  // For thread safety
#ifdef _WIN32
    // Windows handles would go here
#else
    int fd_;  // POSIX file descriptor for memory mapping
#endif
};

// Memory-mapped ring buffer WAL manager
class MMRingBufferWalManager : public WalManager {
public:
    explicit MMRingBufferWalManager(TaskScheduler* scheduler = nullptr);
    ~MMRingBufferWalManager() override;

    Status Open(const WalOptions& options) override;
    Status Close() override;
    Status WriteEntry(const WalEntry& entry) override;
    Status WriteBatch(const std::vector<WalEntry>& entries) override;
    uint64_t GetCurrentSequence() const override;
    Status Sync() override;
    Status Rotate() override;
    Status Recover(std::function<Status(const WalEntry&)> callback,
                  uint64_t start_sequence = 0) override;
    Status Truncate(uint64_t sequence_number) override;
    Status GetStats(std::string* stats) const override;
    Status ListFiles(std::vector<std::string>* files) const override;

private:
    Status EnsureFileExists();
    Status RecoverFromFile(std::function<Status(const WalEntry&)> callback,
                          uint64_t start_sequence);

    WalOptions options_;
    MMRingBufferWalOptions mm_options_;
    TaskScheduler* scheduler_;
    std::unique_ptr<MMRingBufferWalFile> wal_file_;
    std::atomic<uint64_t> current_sequence_;
    bool is_open_;
    mutable std::mutex mutex_;
};

// Factory function for memory-mapped ring buffer WAL
std::unique_ptr<WalManager> CreateMMRingBufferWalManager(TaskScheduler* scheduler = nullptr);

// Factory functions
std::unique_ptr<WalManager> CreateWalManager();
std::unique_ptr<WalManager> CreateWalManager(TaskScheduler* scheduler);
std::unique_ptr<WalFile> CreateWalFile(const std::string& path, uint64_t file_id);

// Utility functions
uint32_t ComputeChecksum(const WalEntry& entry);
bool ValidateChecksum(const WalEntry& entry);

} // namespace marble
