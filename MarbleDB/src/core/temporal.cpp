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

#include "marble/temporal.h"
#include "marble/table.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <mutex>
#include <shared_mutex>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace fs = std::filesystem;
namespace cp = arrow::compute;

namespace marble {

// SnapshotId implementation

std::string SnapshotId::ToString() const {
    return std::to_string(timestamp) + "_" + std::to_string(version);
}

SnapshotId SnapshotId::FromString(const std::string& str) {
    size_t pos = str.find('_');
    if (pos == std::string::npos) {
        return SnapshotId(0, 0);
    }

    uint64_t ts = std::stoull(str.substr(0, pos));
    uint64_t ver = std::stoull(str.substr(pos + 1));
    return SnapshotId(ts, ver);
}

SnapshotId SnapshotId::Latest() {
    return SnapshotId(UINT64_MAX, UINT64_MAX);
}

// TemporalQueryBuilder implementation

TemporalQueryBuilder::TemporalQueryBuilder() = default;

TemporalQueryBuilder& TemporalQueryBuilder::AsOf(const SnapshotId& snapshot) {
    spec_.as_of_snapshot = snapshot;
    return *this;
}

TemporalQueryBuilder& TemporalQueryBuilder::SystemTimeBetween(uint64_t start, uint64_t end) {
    spec_.system_time_start = start;
    spec_.system_time_end = end;
    return *this;
}

TemporalQueryBuilder& TemporalQueryBuilder::SystemTimeFrom(uint64_t start) {
    spec_.system_time_start = start;
    return *this;
}

TemporalQueryBuilder& TemporalQueryBuilder::SystemTimeTo(uint64_t end) {
    spec_.system_time_end = end;
    return *this;
}

TemporalQueryBuilder& TemporalQueryBuilder::ValidTimeBetween(uint64_t start, uint64_t end) {
    spec_.valid_time_start = start;
    spec_.valid_time_end = end;
    return *this;
}

TemporalQueryBuilder& TemporalQueryBuilder::ValidTimeFrom(uint64_t start) {
    spec_.valid_time_start = start;
    return *this;
}

TemporalQueryBuilder& TemporalQueryBuilder::ValidTimeTo(uint64_t end) {
    spec_.valid_time_end = end;
    return *this;
}

TemporalQueryBuilder& TemporalQueryBuilder::IncludeDeleted(bool include) {
    spec_.include_deleted = include;
    return *this;
}

TemporalQuerySpec TemporalQueryBuilder::Build() const {
    return spec_;
}

// TimeTravel utilities

uint64_t TimeTravel::Now() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
}

uint64_t TimeTravel::ParseTimestamp(const std::string& timestamp_str) {
    // Simple ISO 8601 parser - in production, use a proper library
    std::tm tm = {};
    std::istringstream ss(timestamp_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (ss.fail()) {
        return 0;
    }

    auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    return std::chrono::duration_cast<std::chrono::microseconds>(
        tp.time_since_epoch()).count();
}

std::string TimeTravel::FormatTimestamp(uint64_t timestamp) {
    auto tp = std::chrono::system_clock::time_point(
        std::chrono::microseconds(timestamp));
    auto time_t = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::localtime(&time_t);

    std::ostringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return ss.str();
}

SnapshotId TimeTravel::GenerateSnapshotId() {
    return SnapshotId(Now(), 0);  // Version will be set by the table
}

// Forward declarations
class TemporalTableImpl;
class TemporalDatabaseImpl;

/**
 * @brief Temporal table implementation with bitemporal versioning
 */
class TemporalTableImpl : public TemporalTable {
public:
    TemporalTableImpl(const std::string& base_path, const TableSchema& schema, TemporalModel model)
        : base_path_(base_path)
        , schema_(schema)
        , model_(model)
        , current_version_(0) {
        table_path_ = base_path_ + "/" + schema_.table_name;
        fs::create_directories(table_path_);

        // Initialize with system time columns if needed
        InitializeTemporalSchema();
        LoadSnapshots();
    }

    TemporalModel GetTemporalModel() const override {
        return model_;
    }

    Status CreateSnapshot(SnapshotId* snapshot_id) override {
        std::unique_lock<std::shared_mutex> lock(snapshot_mutex_);

        uint64_t now = TimeTravel::Now();
        current_version_++;

        SnapshotId new_snapshot(now, current_version_);
        snapshots_.push_back(new_snapshot);

        // Save snapshot metadata
        SaveSnapshotMetadata();

        *snapshot_id = new_snapshot;
        return Status::OK();
    }

    Status ListSnapshots(std::vector<SnapshotId>* snapshots) const override {
        std::shared_lock<std::shared_mutex> lock(snapshot_mutex_);
        *snapshots = snapshots_;
        return Status::OK();
    }

    Status DeleteSnapshot(const SnapshotId& snapshot_id) override {
        std::unique_lock<std::shared_mutex> lock(snapshot_mutex_);

        auto it = std::find(snapshots_.begin(), snapshots_.end(), snapshot_id);
        if (it == snapshots_.end()) {
            return Status::NotFound("Snapshot not found");
        }

        snapshots_.erase(it);
        SaveSnapshotMetadata();
        return Status::OK();
    }

    Status TemporalScan(const TemporalQuerySpec& temporal_spec,
                       const ScanSpec& scan_spec,
                       std::unique_ptr<QueryResult>* result) override {
        // For now, implement basic temporal scanning
        // TODO: Implement full bitemporal reconstruction

        // Build enhanced scan spec with temporal filters
        ScanSpec enhanced_spec = scan_spec;

        // Add temporal filters based on the spec
        if (temporal_spec.as_of_snapshot.timestamp != 0) {
            // AS OF snapshot filtering
            // TODO: Implement proper snapshot reconstruction
        }

        if (temporal_spec.system_time_start > 0 || temporal_spec.system_time_end < UINT64_MAX) {
            // System time filtering
            // TODO: Implement system time filtering
        }

        // For MVP, just do regular scan
        return Scan(enhanced_spec, result);
    }

    Status TemporalInsert(const arrow::RecordBatch& batch,
                         const TemporalMetadata& metadata) override {
        // Add temporal metadata to the batch
        auto enhanced_batch = AddTemporalMetadata(batch, metadata);

        // Create snapshot if needed
        SnapshotId snapshot;
        if (metadata.created_snapshot.timestamp == 0) {
            CreateSnapshot(&snapshot);
        } else {
            snapshot = metadata.created_snapshot;
        }

        // Insert with temporal versioning
        return Append(*enhanced_batch);
    }

    Status TemporalDelete(const std::string& predicate,
                         const TemporalMetadata& metadata) override {
        // For MVP, implement soft delete
        // TODO: Implement full temporal delete with proper versioning

        // Mark records as deleted in the current snapshot
        SnapshotId snapshot;
        CreateSnapshot(&snapshot);

        // TODO: Actually implement the delete logic
        return Status::NotImplemented("Temporal delete not yet implemented");
    }

    Status TemporalUpdate(const std::string& predicate,
                         const std::unordered_map<std::string, std::string>& updates,
                         const TemporalMetadata& metadata) override {
        // For MVP, implement as insert + soft delete
        // TODO: Implement full temporal update

        return Status::NotImplemented("Temporal update not yet implemented");
    }

    Status GetTemporalStats(std::unordered_map<std::string, std::string>* stats) const override {
        std::shared_lock<std::shared_mutex> lock(snapshot_mutex_);

        (*stats)["temporal_model"] = std::to_string(static_cast<int>(model_));
        (*stats)["snapshot_count"] = std::to_string(snapshots_.size());
        (*stats)["current_version"] = std::to_string(current_version_);

        if (!snapshots_.empty()) {
            (*stats)["latest_snapshot"] = snapshots_.back().ToString();
            (*stats)["earliest_snapshot"] = snapshots_.front().ToString();
        }

        // Get base table stats
        std::unordered_map<std::string, std::string> base_stats;
        GetStats(&base_stats);
        stats->insert(base_stats.begin(), base_stats.end());

        return Status::OK();
    }

private:
    std::shared_ptr<arrow::RecordBatch> AddTemporalMetadata(
        const arrow::RecordBatch& batch, const TemporalMetadata& metadata) {

        // For bitemporal tables, add system time columns
        if (model_ == TemporalModel::kBitemporal || model_ == TemporalModel::kSystemTime) {
            // Add system time columns to schema and data
            // TODO: Implement proper temporal metadata addition
        }

        // For now, just return a copy of the original batch
        // TODO: Implement proper temporal metadata addition
        // FIXME: Simplified for MVP - proper error handling needed
        return arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), batch.columns());
    }

    void InitializeTemporalSchema() {
        // Add temporal columns to schema if needed
        std::vector<std::shared_ptr<arrow::Field>> fields = schema_.arrow_schema->fields();

        if (model_ == TemporalModel::kBitemporal || model_ == TemporalModel::kSystemTime) {
            // Add system time versioning columns
            fields.push_back(arrow::field("_system_time_start", arrow::timestamp(arrow::TimeUnit::MICRO)));
            fields.push_back(arrow::field("_system_time_end", arrow::timestamp(arrow::TimeUnit::MICRO)));
            fields.push_back(arrow::field("_system_version", arrow::int64()));
        }

        if (model_ == TemporalModel::kBitemporal || model_ == TemporalModel::kValidTime) {
            // Add valid time columns
            fields.push_back(arrow::field("_valid_time_start", arrow::timestamp(arrow::TimeUnit::MICRO)));
            fields.push_back(arrow::field("_valid_time_end", arrow::timestamp(arrow::TimeUnit::MICRO)));
        }

        if (model_ != TemporalModel::kUnitemporal) {
            fields.push_back(arrow::field("_is_deleted", arrow::boolean()));
        }

        temporal_schema_ = arrow::schema(fields);
    }

    void LoadSnapshots() {
        std::string snapshot_file = table_path_ + "/snapshots.json";
        if (!fs::exists(snapshot_file)) {
            return;
        }

        try {
            std::ifstream file(snapshot_file);
            json j;
            file >> j;

            snapshots_.clear();
            for (const auto& snapshot_json : j["snapshots"]) {
                std::string snapshot_str = snapshot_json;
                snapshots_.push_back(SnapshotId::FromString(snapshot_str));
            }

            current_version_ = j.value("current_version", 0ULL);

        } catch (const std::exception&) {
            // Reset to empty state on error
            snapshots_.clear();
            current_version_ = 0;
        }
    }

    void SaveSnapshotMetadata() {
        std::string snapshot_file = table_path_ + "/snapshots.json";

        json j;
        j["current_version"] = current_version_;

        json snapshot_array = json::array();
        for (const auto& snapshot : snapshots_) {
            snapshot_array.push_back(snapshot.ToString());
        }
        j["snapshots"] = snapshot_array;

        std::ofstream file(snapshot_file);
        file << j.dump(2);
    }

    // Base table functionality - we'll create a simple in-memory table for now
    std::string GetName() const { return schema_.table_name; }
    const TableSchema& GetSchema() const { return schema_; }

    Status Append(const arrow::RecordBatch& batch) {
        // For MVP, just store in memory
        // FIXME: Simplified for MVP - proper error handling needed
        auto batch_ptr = arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), batch.columns());

        if (!table_data_) {
            // Create table from record batch
            // FIXME: Simplified for MVP - proper error handling needed
            auto table_result = arrow::Table::FromRecordBatches({batch_ptr});
            table_data_ = table_result.ValueUnsafe();
        } else {
            // Concatenate with existing data
            // FIXME: Simplified for MVP - proper error handling needed
            auto new_table_result = arrow::Table::FromRecordBatches({batch_ptr});
            auto new_table = new_table_result.ValueUnsafe();
            // FIXME: Simplified for MVP - proper error handling needed
            auto combined = arrow::ConcatenateTables({table_data_, new_table});
            table_data_ = combined.ValueUnsafe();
        }
        return Status::OK();
    }

    Status Scan(const ScanSpec& spec, std::unique_ptr<QueryResult>* result) {
        if (!table_data_) {
            auto empty_table_result = arrow::Table::MakeEmpty(schema_.arrow_schema);
            if (!empty_table_result.ok()) {
                return Status::FromArrowStatus(empty_table_result.status());
            }
            return TableQueryResult::Create(empty_table_result.ValueUnsafe(), result);
        }

        // Simple scan - return all data for now
        return TableQueryResult::Create(table_data_, result);
    }

    Status GetStats(std::unordered_map<std::string, std::string>* stats) const {
        if (!table_data_) {
            (*stats)["row_count"] = "0";
            (*stats)["column_count"] = std::to_string(schema_.arrow_schema->num_fields());
            return Status::OK();
        }

        (*stats)["row_count"] = std::to_string(table_data_->num_rows());
        (*stats)["column_count"] = std::to_string(table_data_->num_columns());
        return Status::OK();
    }

    std::string base_path_;
    std::string table_path_;
    TableSchema schema_;
    std::shared_ptr<arrow::Schema> temporal_schema_;
    TemporalModel model_;

    mutable std::shared_mutex snapshot_mutex_;
    std::vector<SnapshotId> snapshots_;
    uint64_t current_version_;

    // In-memory table data for MVP
    std::shared_ptr<arrow::Table> table_data_;
};

/**
 * @brief Temporal database implementation
 */
class TemporalDatabaseImpl : public TemporalDatabase {
public:
    TemporalDatabaseImpl(const std::string& base_path)
        : base_path_(base_path) {
        fs::create_directories(base_path_);
        LoadTables();
    }

    Status CreateTemporalTable(const std::string& table_name,
                              const TableSchema& schema,
                              TemporalModel model) override {
        std::unique_lock<std::shared_mutex> lock(tables_mutex_);

        if (tables_.find(table_name) != tables_.end()) {
            return Status::AlreadyExists("Table already exists: " + table_name);
        }

        auto table = std::make_shared<TemporalTableImpl>(base_path_, schema, model);
        tables_[table_name] = table;

        // Save table metadata
        SaveTableMetadata(table_name, schema, model);

        return Status::OK();
    }

    Status GetTemporalTable(const std::string& table_name,
                           std::shared_ptr<TemporalTable>* table) override {
        std::shared_lock<std::shared_mutex> lock(tables_mutex_);

        auto it = tables_.find(table_name);
        if (it == tables_.end()) {
            return Status::NotFound("Table not found: " + table_name);
        }

        *table = it->second;
        return Status::OK();
    }

    Status CreateDatabaseSnapshot(SnapshotId* snapshot_id) override {
        std::unique_lock<std::shared_mutex> lock(tables_mutex_);

        // Create snapshot across all tables
        uint64_t now = TimeTravel::Now();
        uint64_t version = 0;

        for (const auto& [name, table] : tables_) {
            SnapshotId table_snapshot;
            auto status = table->CreateSnapshot(&table_snapshot);
            if (!status.ok()) {
                return status;
            }
            version = std::max(version, table_snapshot.version);
        }

        *snapshot_id = SnapshotId(now, version);
        return Status::OK();
    }

    Status RestoreDatabaseSnapshot(const SnapshotId& snapshot_id) override {
        // TODO: Implement database snapshot restoration
        return Status::NotImplemented("Database snapshot restoration not yet implemented");
    }

private:
    void SaveTableMetadata(const std::string& table_name, const TableSchema& schema, TemporalModel model) {
        std::string metadata_file = base_path_ + "/tables.json";

        // Load existing metadata
        json j;
        if (fs::exists(metadata_file)) {
            std::ifstream file(metadata_file);
            file >> j;
        }

        // Add table metadata
        json table_meta;
        table_meta["schema"] = schema.table_name;
        table_meta["temporal_model"] = static_cast<int>(model);
        table_meta["time_column"] = schema.time_column;
        table_meta["time_partition"] = static_cast<int>(schema.time_partition);

        j["tables"][table_name] = table_meta;

        // Save metadata
        std::ofstream file(metadata_file);
        file << j.dump(2);
    }

    void LoadTables() {
        std::string metadata_file = base_path_ + "/tables.json";
        if (!fs::exists(metadata_file)) {
            return;
        }

        try {
            std::ifstream file(metadata_file);
            json j;
            file >> j;

            if (j.contains("tables")) {
                for (const auto& [table_name, table_meta] : j["tables"].items()) {
                    // Recreate table from metadata
                    std::string schema_name = table_meta["schema"];
                    TemporalModel model = static_cast<TemporalModel>(
                        table_meta.value("temporal_model", static_cast<int>(TemporalModel::kUnitemporal)));

                    // Create basic schema - in production, load full schema
                    auto arrow_schema = arrow::schema({
                        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
                        arrow::field("data", arrow::utf8())
                    });

                    TableSchema schema(schema_name, arrow_schema);
                    schema.time_column = table_meta.value("time_column", "timestamp");
                    schema.time_partition = static_cast<TimePartition>(
                        table_meta.value("time_partition", static_cast<int>(TimePartition::kDaily)));

                    auto table = std::make_shared<TemporalTableImpl>(base_path_, schema, model);
                    tables_[table_name] = table;
                }
            }

        } catch (const std::exception&) {
            // Ignore load errors - start with empty state
        }
    }

    std::string base_path_;
    mutable std::shared_mutex tables_mutex_;
    std::unordered_map<std::string, std::shared_ptr<TemporalTable>> tables_;
};

// Factory functions

std::shared_ptr<TemporalDatabase> CreateTemporalDatabase(const std::string& base_path) {
    return std::make_shared<TemporalDatabaseImpl>(base_path);
}

std::shared_ptr<TemporalTable> CreateTemporalTable(const std::string& base_path,
                                                  const TableSchema& schema,
                                                  TemporalModel model) {
    return std::make_shared<TemporalTableImpl>(base_path, schema, model);
}

} // namespace marble
