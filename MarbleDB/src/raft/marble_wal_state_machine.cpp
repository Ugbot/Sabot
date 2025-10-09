/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "marble/raft.h"
#include "marble/status.h"
#include "marble/wal.h"

#include <memory>
#include <mutex>
#include <unordered_map>
#include <iostream>

namespace marble {

// WAL Replication State Machine
// This state machine handles WAL entry replication across the cluster
class MarbleWalStateMachine : public RaftStateMachine {
public:
    explicit MarbleWalStateMachine(std::unique_ptr<WalManager> wal_manager)
        : wal_manager_(std::move(wal_manager))
        , last_applied_index_(0)
        , demo_mode_(wal_manager_ == nullptr) {

        // If we have a real WAL manager, we can operate in production mode
        if (wal_manager_) {
            demo_mode_ = false;
        }
    }

    ~MarbleWalStateMachine() override = default;

    marble::Status ApplyOperation(const RaftOperation& operation) override {
        std::lock_guard<std::mutex> lock(mutex_);

        if (operation.type != RaftOperationType::kWalEntry) {
            // This state machine only handles WAL entries
            return marble::Status::InvalidArgument("Invalid operation type for WAL state machine");
        }

        try {
            if (demo_mode_) {
                // Demo mode: just simulate applying the operation
                last_applied_index_ = operation.sequence_number;
                applied_entries_count_++;

                std::cout << "[DEMO] Applied WAL entry seq=" << operation.sequence_number
                          << " with data: " << operation.data
                          << " (total applied: " << applied_entries_count_ << ")" << std::endl;

                return marble::Status::OK();
            } else {
                // Parse the WAL entry from the operation data
                WalEntry wal_entry;
                if (!DeserializeWalEntry(operation.data, &wal_entry)) {
                    return marble::Status::InvalidArgument("Failed to deserialize WAL entry");
                }

                // Apply the WAL entry to the local MarbleDB instance
                auto status = wal_manager_->WriteEntry(wal_entry);
                if (!status.ok()) {
                    std::cerr << "Failed to apply WAL entry: " << status.ToString() << std::endl;
                    return status;
                }

                last_applied_index_ = operation.sequence_number;
                applied_entries_count_++;

                std::cout << "Applied WAL entry seq=" << operation.sequence_number
                          << " to local MarbleDB instance (total applied: "
                          << applied_entries_count_ << ")" << std::endl;

                return marble::Status::OK();
            }

        } catch (const std::exception& e) {
            std::cerr << "Exception applying WAL operation: " << e.what() << std::endl;
            return marble::Status::InternalError(std::string("Exception: ") + e.what());
        }
    }

    marble::Status CreateSnapshot(uint64_t log_index) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Create a snapshot of the current WAL state
            // In a full implementation, this would create a checkpoint
            // For now, just record the snapshot index
            snapshot_indices_.push_back(log_index);
            last_snapshot_index_ = log_index;

            std::cout << "Created WAL snapshot at index: " << log_index << std::endl;

            // In a real implementation, you might:
            // 1. Flush all pending WAL entries to disk
            // 2. Create a checkpoint of the current database state
            // 3. Record the snapshot metadata

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception creating snapshot: ") + e.what());
        }
    }

    marble::Status RestoreFromSnapshot(uint64_t log_index) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Restore from a snapshot
            // In a full implementation, this would restore the database state
            last_snapshot_index_ = log_index;
            applied_entries_count_ = log_index; // Approximation

            std::cout << "Restored WAL state from snapshot at index: " << log_index << std::endl;

            // In a real implementation, you might:
            // 1. Load the snapshot data
            // 2. Rebuild the WAL state
            // 3. Replay any entries after the snapshot

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception restoring snapshot: ") + e.what());
        }
    }

    uint64_t GetLastAppliedIndex() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_applied_index_;
    }

    // Additional WAL-specific methods
    uint64_t GetAppliedEntriesCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return applied_entries_count_;
    }

    uint64_t GetLastSnapshotIndex() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_snapshot_index_;
    }

private:
    bool DeserializeWalEntry(const std::string& data, WalEntry* entry) {
        // Simple deserialization - in a real implementation,
        // this would use proper serialization format
        try {
            // For demo purposes, create a simple WAL entry
            entry->sequence_number = applied_entries_count_ + 1;
            entry->transaction_id = 1;
            entry->entry_type = WalEntryType::kFull;
            entry->timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            // Create a simple key and record for demo
            entry->key = std::make_shared<SimpleKey>(data);
            entry->value = std::make_shared<SimpleRecord>(data, "wal_value");

            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    // Forward declarations for demo key/record types
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

    std::unique_ptr<WalManager> wal_manager_;
    mutable std::mutex mutex_;
    bool demo_mode_;

    uint64_t last_applied_index_;
    uint64_t applied_entries_count_ = 0;
    uint64_t last_snapshot_index_ = 0;
    std::vector<uint64_t> snapshot_indices_;
};

// Schema Change State Machine
// This handles DDL operations like CREATE TABLE, ALTER TABLE, etc.
class MarbleSchemaStateMachine : public RaftStateMachine {
public:
    MarbleSchemaStateMachine()
        : last_applied_index_(0)
        , schema_version_(0) {}

    ~MarbleSchemaStateMachine() override = default;

    marble::Status ApplyOperation(const RaftOperation& operation) override {
        std::lock_guard<std::mutex> lock(mutex_);

        if (operation.type != RaftOperationType::kSchemaChange &&
            operation.type != RaftOperationType::kTableCreate &&
            operation.type != RaftOperationType::kTableDrop &&
            operation.type != RaftOperationType::kIndexCreate &&
            operation.type != RaftOperationType::kIndexDrop) {
            return marble::Status::InvalidArgument("Invalid operation type for schema state machine");
        }

        try {
            // Apply the schema change
            auto status = ApplySchemaChange(operation);
            if (!status.ok()) {
                return status;
            }

            last_applied_index_ = operation.sequence_number;
            schema_version_++;

            std::cout << "Applied schema change: " << operation.data
                      << " (schema version: " << schema_version_ << ")" << std::endl;

            return marble::Status::OK();

        } catch (const std::exception& e) {
            std::cerr << "Exception applying schema operation: " << e.what() << std::endl;
            return marble::Status::InternalError(std::string("Exception: ") + e.what());
        }
    }

    marble::Status CreateSnapshot(uint64_t log_index) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Create a snapshot of the current schema state
            snapshot_schema_versions_[log_index] = schema_version_;
            last_snapshot_index_ = log_index;

            std::cout << "Created schema snapshot at index: " << log_index
                      << " (schema version: " << schema_version_ << ")" << std::endl;

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception creating schema snapshot: ") + e.what());
        }
    }

    marble::Status RestoreFromSnapshot(uint64_t log_index) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Restore schema state from snapshot
            auto it = snapshot_schema_versions_.find(log_index);
            if (it != snapshot_schema_versions_.end()) {
                schema_version_ = it->second;
                last_snapshot_index_ = log_index;
            }

            std::cout << "Restored schema state from snapshot at index: " << log_index
                      << " (schema version: " << schema_version_ << ")" << std::endl;

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception restoring schema snapshot: ") + e.what());
        }
    }

    uint64_t GetLastAppliedIndex() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_applied_index_;
    }

    uint64_t GetSchemaVersion() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return schema_version_;
    }

private:
    marble::Status ApplySchemaChange(const RaftOperation& operation) {
        // In a real implementation, this would:
        // 1. Parse the schema change SQL/DDL
        // 2. Validate the change against current schema
        // 3. Apply the change to the local schema metadata
        // 4. Update table/index definitions
        // 5. Handle dependencies and constraints

        switch (operation.type) {
            case RaftOperationType::kTableCreate:
                std::cout << "Creating table: " << operation.data << std::endl;
                break;
            case RaftOperationType::kTableDrop:
                std::cout << "Dropping table: " << operation.data << std::endl;
                break;
            case RaftOperationType::kIndexCreate:
                std::cout << "Creating index: " << operation.data << std::endl;
                break;
            case RaftOperationType::kIndexDrop:
                std::cout << "Dropping index: " << operation.data << std::endl;
                break;
            case RaftOperationType::kSchemaChange:
                std::cout << "Applying schema change: " << operation.data << std::endl;
                break;
            default:
                return marble::Status::InvalidArgument("Unsupported schema operation type");
        }

        // Store the schema change in our metadata
        schema_changes_.push_back(operation);

        return marble::Status::OK();
    }

    mutable std::mutex mutex_;

    uint64_t last_applied_index_;
    uint64_t schema_version_;
    uint64_t last_snapshot_index_ = 0;

    std::vector<RaftOperation> schema_changes_;
    std::unordered_map<uint64_t, uint64_t> snapshot_schema_versions_;
};

// Factory functions for creating state machines

std::unique_ptr<RaftStateMachine> CreateMarbleWalStateMachine(
    std::unique_ptr<WalManager> wal_manager) {
    return std::make_unique<MarbleWalStateMachine>(std::move(wal_manager));
}

std::unique_ptr<RaftStateMachine> CreateMarbleSchemaStateMachine() {
    return std::make_unique<MarbleSchemaStateMachine>();
}

}  // namespace marble
