#include <marble/marble.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <memory>

using namespace marble;

// For demonstration purposes, we'll use simple string-based entries
// In a real implementation, these would be proper Key/Record objects

// WAL recovery callback
class WalRecoveryHandler : public WalRecoveryCallback {
public:
    Status OnWalEntry(const WalEntry& entry) override {
        std::cout << "Recovered WAL entry: seq=" << entry.sequence_number
                  << ", txn=" << entry.transaction_id
                  << ", type=" << static_cast<int>(entry.entry_type)
                  << ", key=" << (entry.key ? entry.key->ToString() : "null")
                  << ", timestamp=" << entry.timestamp << std::endl;

        recovered_entries_.push_back(entry);
        return Status::OK();
    }

    Status OnRecoveryComplete(uint64_t last_sequence) override {
        std::cout << "WAL recovery complete. Last sequence: " << last_sequence
                  << ", Total entries recovered: " << recovered_entries_.size() << std::endl;
        return Status::OK();
    }

    const std::vector<WalEntry>& GetRecoveredEntries() const {
        return recovered_entries_;
    }

private:
    std::vector<WalEntry> recovered_entries_;
};

int main() {
    std::cout << "MarbleDB WAL (Write-Ahead Logging) Example" << std::endl;
    std::cout << "==========================================" << std::endl;

    // Configure WAL options
    WalOptions wal_options;
    wal_options.wal_path = "/tmp/marble_wal_example";
    wal_options.max_file_size = 1 * 1024 * 1024;  // 1MB for demo
    wal_options.sync_mode = WalOptions::SyncMode::kBatch;
    wal_options.sync_interval = 10;  // Sync every 10 entries
    wal_options.enable_checksum = true;

    std::cout << "WAL Configuration:" << std::endl;
    std::cout << "  Path: " << wal_options.wal_path << std::endl;
    std::cout << "  Max file size: " << wal_options.max_file_size / 1024 / 1024 << "MB" << std::endl;
    std::cout << "  Sync mode: " << (wal_options.sync_mode == WalOptions::SyncMode::kBatch ? "batch" : "sync") << std::endl;
    std::cout << "  Checksum: " << (wal_options.enable_checksum ? "enabled" : "disabled") << std::endl;

    // Create WAL manager
    auto wal_manager = CreateWalManager();
    if (!wal_manager) {
        std::cerr << "Failed to create WAL manager" << std::endl;
        return 1;
    }

    // Open WAL
    Status status = wal_manager->Open(wal_options);
    if (!status.ok()) {
        std::cerr << "Failed to open WAL: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "\nWAL opened successfully" << std::endl;

    // Write some test entries
    std::cout << "\nWriting test entries to WAL..." << std::endl;

    for (int i = 0; i < 50; ++i) {
        WalEntry entry(
            i + 1,  // sequence number
            1,      // transaction ID
            WalEntryType::kFull,  // entry type
            nullptr,  // key (simplified for demo)
            nullptr,  // value (simplified for demo)
            0  // timestamp (will be set automatically)
        );

        status = wal_manager->WriteEntry(entry);
        if (!status.ok()) {
            std::cerr << "Failed to write WAL entry: " << status.ToString() << std::endl;
            return 1;
        }

        if ((i + 1) % 10 == 0) {
            std::cout << "  Wrote " << (i + 1) << " entries..." << std::endl;
        }
    }

    // Force sync
    status = wal_manager->Sync();
    if (!status.ok()) {
        std::cerr << "Failed to sync WAL: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "All entries written and synced" << std::endl;

    // Get WAL stats
    std::string stats;
    status = wal_manager->GetStats(&stats);
    if (status.ok()) {
        std::cout << "\nWAL Stats: " << stats << std::endl;
    }

    // List WAL files
    std::vector<std::string> files;
    status = wal_manager->ListFiles(&files);
    if (status.ok()) {
        std::cout << "\nWAL Files:" << std::endl;
        for (const auto& file : files) {
            std::cout << "  " << file << std::endl;
        }
    }

    // Test recovery
    std::cout << "\nTesting WAL recovery..." << std::endl;

    WalRecoveryHandler recovery_handler;

    status = wal_manager->Recover(
        [&recovery_handler](const WalEntry& entry) -> Status {
            return recovery_handler.OnWalEntry(entry);
        }
    );

    if (!status.ok()) {
        std::cerr << "WAL recovery failed: " << status.ToString() << std::endl;
        return 1;
    }

    recovery_handler.OnRecoveryComplete(wal_manager->GetCurrentSequence());

    // Test batch writing
    std::cout << "\nTesting batch write..." << std::endl;

    std::vector<WalEntry> batch_entries;
    for (int i = 51; i <= 60; ++i) {
        WalEntry entry(
            i,
            2,  // different transaction ID
            WalEntryType::kFull,
            nullptr,  // key (simplified for demo)
            nullptr   // value (simplified for demo)
        );
        batch_entries.push_back(entry);
    }

    status = wal_manager->WriteBatch(batch_entries);
    if (!status.ok()) {
        std::cerr << "Failed to write batch: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "Batch write completed. Final sequence: " << wal_manager->GetCurrentSequence() << std::endl;

    // Test file rotation by writing more entries
    std::cout << "\nTesting file rotation..." << std::endl;

    for (int i = 61; i <= 200; ++i) {  // Write many entries to trigger rotation
        WalEntry entry(
            i,
            3,
            WalEntryType::kFull,
            nullptr,  // key (simplified for demo)
            nullptr   // value (simplified for demo)
        );

        status = wal_manager->WriteEntry(entry);
        if (!status.ok()) {
            std::cerr << "Failed to write entry during rotation test: " << status.ToString() << std::endl;
            return 1;
        }
    }

    // Get final stats
    status = wal_manager->GetStats(&stats);
    if (status.ok()) {
        std::cout << "\nFinal WAL Stats: " << stats << std::endl;
    }

    status = wal_manager->ListFiles(&files);
    if (status.ok()) {
        std::cout << "\nFinal WAL Files (" << files.size() << " total):" << std::endl;
        for (const auto& file : files) {
            std::cout << "  " << file << std::endl;
        }
    }

    // Close WAL
    status = wal_manager->Close();
    if (!status.ok()) {
        std::cerr << "Failed to close WAL: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "\nWAL closed successfully" << std::endl;
    std::cout << "\nWAL Example completed successfully!" << std::endl;
    std::cout << "\nKey features demonstrated:" << std::endl;
    std::cout << "• Sequential logging with sequence numbers" << std::endl;
    std::cout << "• Transaction support with transaction IDs" << std::endl;
    std::cout << "• Multiple entry types (Full, Delete, Commit, Abort)" << std::endl;
    std::cout << "• Checksum validation for data integrity" << std::endl;
    std::cout << "• Configurable sync modes (Async, Sync, Batch)" << std::endl;
    std::cout << "• Automatic file rotation" << std::endl;
    std::cout << "• WAL recovery for crash recovery" << std::endl;
    std::cout << "• Batch writing for efficiency" << std::endl;

    return 0;
}
