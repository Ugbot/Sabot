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
#include "marble/db.h"
#include "marble/status.h"
#include "marble/wal.h"
#include <nlohmann/json.hpp>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <sstream>

namespace marble {

//==============================================================================
// Operation Types for MarbleDB Distributed Operations
//==============================================================================

enum class MarbleOperationType {
    kPut,              // Put single key-value pair
    kDelete,           // Delete key
    kInsertBatch,      // Insert batch of records
    kCreateTable,      // Create table/column family
    kDropTable,        // Drop table/column family
    kMerge,            // Merge operation
    kWriteBatch,       // Write batch of operations
    kCheckpoint,       // Checkpoint operation
    kSchemaChange      // Schema modification
};

/**
 * @brief Serialized MarbleDB operation for RAFT replication
 */
struct MarbleOperation {
    MarbleOperationType type;
    std::string table_name;
    std::string key_data;
    std::string value_data;
    uint64_t timestamp;
    std::unordered_map<std::string, std::string> metadata;

    // Serialize to JSON string for Raft log
    std::string Serialize() const {
        nlohmann::json j;
        j["type"] = static_cast<int>(type);
        j["table_name"] = table_name;
        j["key_data"] = key_data;
        j["value_data"] = value_data;
        j["timestamp"] = timestamp;
        j["metadata"] = metadata;
        return j.dump();
    }

    // Deserialize from JSON string
    static std::unique_ptr<MarbleOperation> Deserialize(const std::string& data) {
        try {
            auto op = std::make_unique<MarbleOperation>();
            nlohmann::json j = nlohmann::json::parse(data);

            op->type = static_cast<MarbleOperationType>(j["type"]);
            op->table_name = j["table_name"];
            op->key_data = j["key_data"];
            op->value_data = j["value_data"];
            op->timestamp = j["timestamp"];
            op->metadata = j["metadata"];

            return op;
        } catch (const std::exception& e) {
            std::cerr << "Failed to deserialize MarbleOperation: " << e.what() << std::endl;
            return nullptr;
        }
    }
};

//==============================================================================
// MarbleDB State Machine Implementation
//==============================================================================

/**
 * @brief Main MarbleDB RAFT State Machine
 *
 * Handles all distributed MarbleDB operations with strong consistency guarantees.
 * Operations are replicated via RAFT and applied to local MarbleDB instance.
 */
class MarbleDBStateMachine : public RaftStateMachine {
public:
    explicit MarbleDBStateMachine(std::shared_ptr<MarbleDB> db)
        : db_(db)
        , last_applied_index_(0)
        , operations_applied_(0) {

        if (!db_) {
            throw std::runtime_error("MarbleDB instance cannot be null");
        }
    }

    ~MarbleDBStateMachine() override = default;

    // Disable copy/move
    MarbleDBStateMachine(const MarbleDBStateMachine&) = delete;
    MarbleDBStateMachine& operator=(const MarbleDBStateMachine&) = delete;

    /**
     * @brief Apply a replicated operation to the local MarbleDB instance
     */
    marble::Status ApplyOperation(const RaftOperation& raft_op) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Deserialize the MarbleDB operation from Raft operation data
            auto marble_op = MarbleOperation::Deserialize(raft_op.data);
            if (!marble_op) {
                return marble::Status::InvalidArgument("Failed to deserialize MarbleDB operation");
            }

            // Apply the operation to the local database
            auto status = ApplyMarbleOperation(*marble_op);
            if (!status.ok()) {
                std::cerr << "Failed to apply MarbleDB operation: " << status.ToString() << std::endl;
                return status;
            }

            // Update metadata
            last_applied_index_ = raft_op.sequence_number;
            operations_applied_++;

            std::cout << "[RAFT] Applied operation seq=" << raft_op.sequence_number
                      << " type=" << static_cast<int>(marble_op->type)
                      << " table=" << marble_op->table_name
                      << " (total applied: " << operations_applied_ << ")" << std::endl;

            return marble::Status::OK();

        } catch (const std::exception& e) {
            std::cerr << "Exception applying MarbleDB operation: " << e.what() << std::endl;
            return marble::Status::InternalError(std::string("Exception: ") + e.what());
        }
    }

    /**
     * @brief Create a snapshot of the current database state
     */
    marble::Status CreateSnapshot(uint64_t log_index) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Create a checkpoint of the current database state
            std::string checkpoint_path = GenerateCheckpointPath(log_index);

            auto status = db_->CreateCheckpoint(checkpoint_path);
            if (!status.ok()) {
                std::cerr << "Failed to create database checkpoint: " << status.ToString() << std::endl;
                return status;
            }

            // Store snapshot metadata
            snapshots_[log_index] = checkpoint_path;
            last_snapshot_index_ = log_index;

            std::cout << "[RAFT] Created database snapshot at index " << log_index
                      << " path: " << checkpoint_path << std::endl;

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception creating snapshot: ") + e.what());
        }
    }

    /**
     * @brief Restore database state from a snapshot
     */
    marble::Status RestoreFromSnapshot(uint64_t log_index) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            auto it = snapshots_.find(log_index);
            if (it == snapshots_.end()) {
                return marble::Status::NotFound("Snapshot not found for index: " + std::to_string(log_index));
            }

            const std::string& checkpoint_path = it->second;

            auto status = db_->RestoreFromCheckpoint(checkpoint_path);
            if (!status.ok()) {
                std::cerr << "Failed to restore from checkpoint: " << status.ToString() << std::endl;
                return status;
            }

            last_snapshot_index_ = log_index;

            std::cout << "[RAFT] Restored database from snapshot at index " << log_index
                      << " path: " << checkpoint_path << std::endl;

            return marble::Status::OK();

        } catch (const std::exception& e) {
            return marble::Status::InternalError(std::string("Exception restoring snapshot: ") + e.what());
        }
    }

    uint64_t GetLastAppliedIndex() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_applied_index_;
    }

    // Additional methods for monitoring and management
    uint64_t GetOperationsApplied() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return operations_applied_;
    }

    uint64_t GetLastSnapshotIndex() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_snapshot_index_;
    }

    std::vector<uint64_t> GetSnapshotIndices() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<uint64_t> indices;
        for (const auto& pair : snapshots_) {
            indices.push_back(pair.first);
        }
        return indices;
    }

private:
    /**
     * @brief Apply a specific MarbleDB operation type
     */
    marble::Status ApplyMarbleOperation(const MarbleOperation& op) {
        switch (op.type) {
            case MarbleOperationType::kPut:
                return ApplyPutOperation(op);
            case MarbleOperationType::kDelete:
                return ApplyDeleteOperation(op);
            case MarbleOperationType::kInsertBatch:
                return ApplyInsertBatchOperation(op);
            case MarbleOperationType::kCreateTable:
                return ApplyCreateTableOperation(op);
            case MarbleOperationType::kDropTable:
                return ApplyDropTableOperation(op);
            case MarbleOperationType::kMerge:
                return ApplyMergeOperation(op);
            case MarbleOperationType::kWriteBatch:
                return ApplyWriteBatchOperation(op);
            case MarbleOperationType::kCheckpoint:
                return ApplyCheckpointOperation(op);
            case MarbleOperationType::kSchemaChange:
                return ApplySchemaChangeOperation(op);
            default:
                return marble::Status::InvalidArgument("Unknown operation type: " +
                                                       std::to_string(static_cast<int>(op.type)));
        }
    }

    marble::Status ApplyPutOperation(const MarbleOperation& op) {
        // Deserialize key and value from operation data
        // This would use proper serialization in production
        auto key = DeserializeKey(op.key_data);
        auto record = DeserializeRecord(op.value_data);

        if (!key || !record) {
            return marble::Status::InvalidArgument("Failed to deserialize key/value for Put operation");
        }

        WriteOptions write_opts;
        return db_->Put(write_opts, record);
    }

    marble::Status ApplyDeleteOperation(const MarbleOperation& op) {
        auto key = DeserializeKey(op.key_data);
        if (!key) {
            return marble::Status::InvalidArgument("Failed to deserialize key for Delete operation");
        }

        WriteOptions write_opts;
        return db_->Delete(write_opts, *key);
    }

    marble::Status ApplyInsertBatchOperation(const MarbleOperation& op) {
        // Deserialize RecordBatch from operation data
        auto batch = DeserializeRecordBatch(op.value_data);
        if (!batch) {
            return marble::Status::InvalidArgument("Failed to deserialize batch for InsertBatch operation");
        }

        return db_->InsertBatch(op.table_name, batch);
    }

    marble::Status ApplyCreateTableOperation(const MarbleOperation& op) {
        // Deserialize table schema from operation data
        auto schema = DeserializeSchema(op.value_data);
        if (!schema) {
            return marble::Status::InvalidArgument("Failed to deserialize schema for CreateTable operation");
        }

        TableSchema table_schema(op.table_name, schema);
        return db_->CreateTable(table_schema);
    }

    marble::Status ApplyDropTableOperation(const MarbleOperation& op) {
        // For drop table, table_name contains the table to drop
        // This would need to be implemented in MarbleDB API
        std::cerr << "DropTable operation not yet implemented" << std::endl;
        return marble::Status::NotImplemented("DropTable operation not implemented");
    }

    marble::Status ApplyMergeOperation(const MarbleOperation& op) {
        auto key = DeserializeKey(op.key_data);
        if (!key) {
            return marble::Status::InvalidArgument("Failed to deserialize key for Merge operation");
        }

        WriteOptions write_opts;
        return db_->Merge(write_opts, *key, op.value_data);
    }

    marble::Status ApplyWriteBatchOperation(const MarbleOperation& op) {
        // Deserialize batch of operations from value_data
        auto operations = DeserializeWriteBatch(op.value_data);
        if (!operations) {
            return marble::Status::InvalidArgument("Failed to deserialize write batch");
        }

        return db_->WriteBatch(WriteOptions{}, *operations);
    }

    marble::Status ApplyCheckpointOperation(const MarbleOperation& op) {
        // Trigger checkpoint operation
        return db_->Flush();  // Flush could trigger checkpoint
    }

    marble::Status ApplySchemaChangeOperation(const MarbleOperation& op) {
        // Apply schema change (DDL operation)
        // This would modify table schemas, indexes, etc.
        std::cerr << "SchemaChange operation not yet fully implemented" << std::endl;
        return marble::Status::NotImplemented("SchemaChange operation not fully implemented");
    }

    // Serialization helpers (simplified for now)
    std::shared_ptr<Key> DeserializeKey(const std::string& data) {
        // In production, this would use proper serialization
        // For now, return a simple string key
        try {
            nlohmann::json j = nlohmann::json::parse(data);
            // Simple key deserialization - extend as needed
            return nullptr; // Placeholder
        } catch (...) {
            return nullptr;
        }
    }

    std::shared_ptr<Record> DeserializeRecord(const std::string& data) {
        // In production, this would deserialize Record objects
        // For now, return null
        return nullptr; // Placeholder
    }

    std::shared_ptr<arrow::RecordBatch> DeserializeRecordBatch(const std::string& data) {
        // In production, this would deserialize Arrow RecordBatch
        // For now, return null
        return nullptr; // Placeholder
    }

    std::shared_ptr<arrow::Schema> DeserializeSchema(const std::string& data) {
        // In production, this would deserialize Arrow Schema
        // For now, return null
        return nullptr; // Placeholder
    }

    std::shared_ptr<std::vector<std::shared_ptr<Record>>> DeserializeWriteBatch(const std::string& data) {
        // In production, this would deserialize batch of records
        // For now, return null
        return nullptr; // Placeholder
    }

    std::string GenerateCheckpointPath(uint64_t log_index) {
        std::stringstream ss;
        ss << "/tmp/marble_checkpoints/snapshot_" << log_index;
        return ss.str();
    }

    std::shared_ptr<MarbleDB> db_;
    mutable std::mutex mutex_;

    uint64_t last_applied_index_;
    uint64_t operations_applied_;
    uint64_t last_snapshot_index_;

    std::unordered_map<uint64_t, std::string> snapshots_; // log_index -> checkpoint_path
};

//==============================================================================
// Factory Functions
//==============================================================================

/**
 * @brief Create a full MarbleDB state machine for RAFT replication
 */
std::unique_ptr<RaftStateMachine> CreateMarbleDBStateMachine(std::shared_ptr<MarbleDB> db) {
    return std::make_unique<MarbleDBStateMachine>(db);
}

/**
 * @brief Create operation for distributed Put
 */
std::unique_ptr<RaftOperation> CreatePutOperation(
    const std::string& table_name,
    std::shared_ptr<Record> record,
    uint64_t sequence_number) {

    auto marble_op = std::make_unique<MarbleOperation>();
    marble_op->type = MarbleOperationType::kPut;
    marble_op->table_name = table_name;
    marble_op->timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Serialize record (simplified)
    // In production: proper serialization
    marble_op->value_data = "serialized_record_placeholder";

    auto raft_op = std::make_unique<RaftOperation>();
    raft_op->type = RaftOperationType::kCustom;
    raft_op->data = marble_op->Serialize();
    raft_op->sequence_number = sequence_number;
    raft_op->timestamp = marble_op->timestamp;

    return raft_op;
}

/**
 * @brief Create operation for distributed InsertBatch
 */
std::unique_ptr<RaftOperation> CreateInsertBatchOperation(
    const std::string& table_name,
    std::shared_ptr<arrow::RecordBatch> batch,
    uint64_t sequence_number) {

    auto marble_op = std::make_unique<MarbleOperation>();
    marble_op->type = MarbleOperationType::kInsertBatch;
    marble_op->table_name = table_name;
    marble_op->timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Serialize batch (simplified)
    // In production: proper Arrow serialization
    marble_op->value_data = "serialized_batch_placeholder";

    auto raft_op = std::make_unique<RaftOperation>();
    raft_op->type = RaftOperationType::kCustom;
    raft_op->data = marble_op->Serialize();
    raft_op->sequence_number = sequence_number;
    raft_op->timestamp = marble_op->timestamp;

    return raft_op;
}

/**
 * @brief Create operation for distributed CreateTable
 */
std::unique_ptr<RaftOperation> CreateCreateTableOperation(
    const TableSchema& schema,
    uint64_t sequence_number) {

    auto marble_op = std::make_unique<MarbleOperation>();
    marble_op->type = MarbleOperationType::kCreateTable;
    marble_op->table_name = schema.table_name;
    marble_op->timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    // Serialize schema (simplified)
    // In production: proper schema serialization
    marble_op->value_data = "serialized_schema_placeholder";

    auto raft_op = std::make_unique<RaftOperation>();
    raft_op->type = RaftOperationType::kTableCreate;
    raft_op->data = marble_op->Serialize();
    raft_op->sequence_number = sequence_number;
    raft_op->timestamp = marble_op->timestamp;

    return raft_op;
}

} // namespace marble
