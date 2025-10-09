#include <marble/wal.h>
#include <marble/task_scheduler.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <memory>

using namespace marble;

// Simple key implementation for demo
class SimpleKey : public Key {
public:
    explicit SimpleKey(const std::string& value) : value_(value) {}

    std::string ToString() const override { return value_; }
    int Compare(const Key& other) const override {
        const SimpleKey& other_key = static_cast<const SimpleKey&>(other);
        return value_.compare(other_key.value_);
    }
    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return arrow::MakeScalar(value_);
    }
    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<SimpleKey>(value_);
    }
    size_t Hash() const override { return std::hash<std::string>()(value_); }

private:
    std::string value_;
};

// Simple record implementation for demo
class SimpleRecord : public Record {
public:
    SimpleRecord(const std::string& key, const std::string& value)
        : key_(key), value_(value) {}

    std::shared_ptr<Key> GetKey() const override {
        return std::make_shared<SimpleKey>(key_);
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override {
        return arrow::RecordBatch::MakeEmpty(nullptr);
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return nullptr;
    }

    std::unique_ptr<RecordRef> AsRecordRef() const override {
        return nullptr;
    }

    size_t Size() const {
        return key_.size() + value_.size();
    }

private:
    std::string key_;
    std::string value_;
};

void demonstrate_ring_buffer_wal() {
    std::cout << "MarbleDB Memory-Mapped Ring Buffer WAL Demo" << std::endl;
    std::cout << "==========================================" << std::endl;

    // Create task scheduler for WAL operations
    auto scheduler = std::make_unique<TaskScheduler>(4); // 4 threads

    // Create memory-mapped ring buffer WAL options
    MMRingBufferWalOptions options;
    options.wal_path = "/tmp/marble_ring_wal.db";
    options.buffer_size = 4 * 1024 * 1024;  // 4MB ring buffer
    options.max_entry_size = 64 * 1024;     // 64KB max entry
    options.enable_checksum = true;

    std::cout << "WAL Configuration:" << std::endl;
    std::cout << "  Path: " << options.wal_path << std::endl;
    std::cout << "  Buffer size: " << options.buffer_size << " bytes" << std::endl;
    std::cout << "  Max entry size: " << options.max_entry_size << " bytes" << std::endl;
    std::cout << "  Checksum enabled: " << (options.enable_checksum ? "Yes" : "No") << std::endl;

    // Create WAL manager
    auto wal_manager = CreateMMRingBufferWalManager(scheduler.get());

    std::cout << "Opening WAL..." << std::endl;
    // Open WAL
    auto status = wal_manager->Open(options);
    if (!status.ok()) {
        std::cerr << "Failed to open WAL: " << status.ToString() << std::endl;
        return;
    }

    std::cout << "WAL opened successfully!" << std::endl;

    // Write some test entries
    std::cout << "\nWriting test entries..." << std::endl;

    for (int i = 1; i <= 10; ++i) {
        WalEntry entry;
        entry.sequence_number = i;
        entry.transaction_id = 1;
        entry.entry_type = WalEntryType::kFull;
        entry.key = std::make_shared<SimpleKey>("key_" + std::to_string(i));
        entry.value = std::make_shared<SimpleRecord>("key_" + std::to_string(i),
                                                   "value_" + std::to_string(i));
        entry.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        status = wal_manager->WriteEntry(entry);
        if (!status.ok()) {
            std::cerr << "Failed to write entry " << i << ": " << status.ToString() << std::endl;
            return;
        }

        std::cout << "  Wrote entry " << i << " (seq: " << entry.sequence_number << ")" << std::endl;
    }

    // Sync to disk
    status = wal_manager->Sync();
    if (!status.ok()) {
        std::cerr << "Failed to sync WAL: " << status.ToString() << std::endl;
        return;
    }

    std::cout << "WAL synced to disk" << std::endl;

    // Get WAL stats
    std::string stats;
    status = wal_manager->GetStats(&stats);
    if (status.ok()) {
        std::cout << "\n" << stats << std::endl;
    }

    // Read back entries
    std::cout << "\nReading back entries..." << std::endl;

    std::vector<WalEntry> recovered_entries;
    status = wal_manager->Recover([&](const WalEntry& entry) {
        recovered_entries.push_back(entry);
        std::cout << "  Recovered entry seq " << entry.sequence_number;
        if (entry.key) {
            std::cout << " key: " << entry.key->ToString();
        }
        std::cout << std::endl;
        return Status::OK();
    }, 0);

    if (!status.ok()) {
        std::cerr << "Failed to recover entries: " << status.ToString() << std::endl;
        return;
    }

    std::cout << "Recovered " << recovered_entries.size() << " entries" << std::endl;

    // Demonstrate ring buffer behavior - write more entries to trigger wraparound
    std::cout << "\nDemonstrating ring buffer wraparound..." << std::endl;
    std::cout << "Writing 100 more entries to fill the ring buffer..." << std::endl;

    for (int i = 11; i <= 110; ++i) {
        WalEntry entry;
        entry.sequence_number = i;
        entry.transaction_id = 1;
        entry.entry_type = WalEntryType::kFull;
        entry.key = std::make_shared<SimpleKey>("key_" + std::to_string(i));
        entry.value = std::make_shared<SimpleRecord>("key_" + std::to_string(i),
                                                   "value_" + std::to_string(i));
        entry.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        status = wal_manager->WriteEntry(entry);
        if (!status.ok()) {
            std::cerr << "Failed to write entry " << i << ": " << status.ToString() << std::endl;
            break;
        }

        if (i % 20 == 0) {
            std::cout << "  Wrote entry " << i << std::endl;
        }
    }

    // Get updated stats
    status = wal_manager->GetStats(&stats);
    if (status.ok()) {
        std::cout << "\nAfter wraparound:\n" << stats << std::endl;
    }

    // Read entries starting from sequence 50
    std::cout << "\nReading entries starting from sequence 50..." << std::endl;
    recovered_entries.clear();
    status = wal_manager->Recover([&](const WalEntry& entry) {
        if (recovered_entries.size() < 10) {  // Limit output
            recovered_entries.push_back(entry);
            std::cout << "  Entry seq " << entry.sequence_number;
            if (entry.key) {
                std::cout << " key: " << entry.key->ToString();
            }
            std::cout << std::endl;
        }
        return Status::OK();
    }, 50);

    if (!status.ok()) {
        std::cerr << "Failed to recover entries from seq 50: " << status.ToString() << std::endl;
    }

    // Close WAL
    status = wal_manager->Close();
    if (!status.ok()) {
        std::cerr << "Failed to close WAL: " << status.ToString() << std::endl;
        return;
    }

    std::cout << "\nWAL closed successfully!" << std::endl;

    // Re-open to demonstrate persistence
    std::cout << "\nRe-opening WAL to demonstrate persistence..." << std::endl;

    status = wal_manager->Open(options);
    if (!status.ok()) {
        std::cerr << "Failed to re-open WAL: " << status.ToString() << std::endl;
        return;
    }

    // Get final stats
    status = wal_manager->GetStats(&stats);
    if (status.ok()) {
        std::cout << "After re-opening:\n" << stats << std::endl;
    }

    // Final recovery to show persistence
    recovered_entries.clear();
    status = wal_manager->Recover([&](const WalEntry& entry) {
        recovered_entries.push_back(entry);
        return Status::OK();
    }, 0);

    if (status.ok()) {
        std::cout << "Recovered " << recovered_entries.size() << " entries after re-opening" << std::endl;
    }

    // Close finally
    wal_manager->Close();

    std::cout << "\nMemory-mapped ring buffer WAL demo completed successfully!" << std::endl;
    std::cout << "\nKey features demonstrated:" << std::endl;
    std::cout << "• Fixed-size ring buffer with automatic wraparound" << std::endl;
    std::cout << "• Memory mapping for high-performance I/O" << std::endl;
    std::cout << "• Offset-based reading and writing" << std::endl;
    std::cout << "• Persistence across open/close cycles" << std::endl;
    std::cout << "• Sequence number tracking" << std::endl;
    std::cout << "• Automatic space management" << std::endl;
}

int main() {
    try {
        demonstrate_ring_buffer_wal();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
