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

#include "marble/table.h"
#include "marble/analytics.h"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/logging.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <mutex>
#include <vector>
#include <iostream>

namespace fs = std::filesystem;
namespace cp = arrow::compute;

namespace marble {

// Forward declarations
class FilePartition;
class FileTable;
class FileDatabase;

/**
 * @brief File-based partition implementation
 */
class FilePartition : public Partition {
public:
    FilePartition(const std::string& table_path, const std::string& partition_id,
                 const TableSchema& schema)
        : table_path_(table_path)
        , partition_id_(partition_id)
        , schema_(schema)
        , row_count_(0)
        , size_bytes_(0) {
        partition_path_ = table_path_ + "/partitions/" + partition_id_;
        fs::create_directories(partition_path_);
    }

    std::string GetPartitionId() const override {
        return partition_id_;
    }

    Status Append(const arrow::RecordBatch& batch) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            // Create filename based on sequence number
            std::string filename = partition_path_ + "/data_" +
                                 std::to_string(file_count_) + ".feather";

            // Write to Feather format
            auto outfile_result = arrow::io::FileOutputStream::Open(filename);
            if (!outfile_result.ok()) {
                return Status::FromArrowStatus(outfile_result.status());
            }
            auto outfile = outfile_result.ValueUnsafe();

            auto writer_result = arrow::ipc::MakeFileWriter(outfile, batch.schema());
            if (!writer_result.ok()) {
                return Status::FromArrowStatus(writer_result.status());
            }
            auto writer = writer_result.ValueUnsafe();

            auto write_result = writer->WriteRecordBatch(batch);
            if (!write_result.ok()) {
                return Status::FromArrowStatus(write_result);
            }

            auto close_result = writer->Close();
            if (!close_result.ok()) {
                return Status::FromArrowStatus(close_result);
            }

            // Update metadata
            file_count_++;
            row_count_ += batch.num_rows();

            // Get file size
            if (fs::exists(filename)) {
                size_bytes_ += fs::file_size(filename);
            }

            // Update time range
            UpdateTimeRange(batch);

            // Build indexes for this batch
            BuildIndexes(batch);

            return Status::OK();

        } catch (const std::exception& e) {
            return Status::IOError(std::string("Failed to append batch: ") + e.what());
        }
    }

    Status Scan(const ScanSpec& spec, std::unique_ptr<QueryResult>* result) override {
        std::lock_guard<std::mutex> lock(mutex_);

        try {
            std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

            // Read all feather files in this partition
            for (const auto& entry : fs::directory_iterator(partition_path_)) {
                if (entry.path().extension() == ".feather") {
                    auto batch = ReadFeatherFile(entry.path().string());
                    if (batch) {
                        batches.push_back(batch);
                    }
                }
            }

            if (batches.empty()) {
                auto empty_table_result = arrow::Table::MakeEmpty(schema_.arrow_schema);
                if (!empty_table_result.ok()) {
                    return Status::FromArrowStatus(empty_table_result.status());
                }
                auto empty_table = empty_table_result.ValueUnsafe();
                return TableQueryResult::Create(empty_table, result);
            }

            // Concatenate all batches
            auto table_result = arrow::Table::FromRecordBatches(batches);
            if (!table_result.ok()) {
                return Status::FromArrowStatus(table_result.status());
            }
            auto table = table_result.ValueUnsafe();

            // Apply filters if specified
            auto filtered_result = ApplyFilters(table, spec);
            if (!filtered_result.status().ok()) {
                return Status::FromArrowStatus(filtered_result.status());
            }
            table = filtered_result.ValueUnsafe();

            // TODO: Implement column projection and limit
            // For Phase 1 MVP, skip these optimizations to get basic functionality working
            // Column projection and limit will be added in Phase 2

            return TableQueryResult::Create(table, result);

        } catch (const std::exception& e) {
            return Status::InternalError(std::string("Failed to scan partition: ") + e.what());
        }
    }

    int64_t GetRowCount() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return row_count_;
    }

    int64_t GetSizeBytes() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        return size_bytes_;
    }

    Status GetTimeRange(int64_t* min_time, int64_t* max_time) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        *min_time = min_time_;
        *max_time = max_time_;
        return Status::OK();
    }

    void BuildIndexes(const arrow::RecordBatch& batch) {
        // Build indexes for each column
        for (int i = 0; i < batch.num_columns(); ++i) {
            auto field = batch.schema()->field(i);
            auto column_name = field->name();
            auto array = batch.column(i);

            ColumnIndex index(column_name);
            if (index.BuildFromArray(*array).ok()) {
                // Merge with existing index or create new one
                auto it = column_indexes_.find(column_name);
                if (it == column_indexes_.end()) {
                    column_indexes_[column_name] = std::move(index);
                } else {
                    // TODO: Merge indexes from multiple batches
                    // For now, just replace with latest
                    it->second = std::move(index);
                }
            }
        }
    }

private:
    std::shared_ptr<arrow::RecordBatch> ReadFeatherFile(const std::string& filename) {
        try {
            auto infile_result = arrow::io::ReadableFile::Open(filename);
            if (!infile_result.ok()) {
                return nullptr;  // Skip files that can't be opened
            }
            auto infile = infile_result.ValueUnsafe();

            auto reader_result = arrow::ipc::RecordBatchFileReader::Open(infile);
            if (!reader_result.ok()) {
                return nullptr;  // Skip corrupted files
            }
            auto reader = reader_result.ValueUnsafe();

            auto batch_result = reader->ReadRecordBatch(0);
            if (!batch_result.ok()) {
                return nullptr;  // Skip corrupted batches
            }
            return batch_result.ValueUnsafe();
        } catch (const std::exception&) {
            return nullptr;  // Skip corrupted files
        }
    }

    arrow::Result<std::shared_ptr<arrow::Table>> ApplyFilters(
        std::shared_ptr<arrow::Table> table, const ScanSpec& spec) {

        if (spec.filters.empty() && spec.time_filter.empty()) {
            return table;
        }

        // TODO: Implement proper filter pushdown using Arrow compute
        // For Phase 1 MVP, we skip filtering and return all data
        // This allows us to get the basic storage and retrieval working first
        return table;
    }

    void UpdateTimeRange(const arrow::RecordBatch& batch) {
        // Find timestamp column
        auto timestamp_col = batch.GetColumnByName(schema_.time_column);
        if (!timestamp_col) return;

        // Get min/max timestamps
        // TODO: Implement proper min/max extraction
        // For now, assume first/last values
        if (timestamp_col->length() > 0) {
            // This is a simplified implementation
            // TODO: Use Arrow compute functions for proper min/max
            if (timestamp_col->type()->id() == arrow::Type::TIMESTAMP) {
                min_time_ = std::min(min_time_, 0LL);  // Placeholder
                max_time_ = std::max(max_time_, 0LL);  // Placeholder
            }
        }
    }

    std::string table_path_;
    std::string partition_id_;
    std::string partition_path_;
    TableSchema schema_;
    mutable std::mutex mutex_;

    int64_t row_count_;
    int64_t size_bytes_;
    int file_count_ = 0;
    int64_t min_time_ = INT64_MAX;
    int64_t max_time_ = INT64_MIN;

    // Column indexes for analytical queries
    std::unordered_map<std::string, ColumnIndex> column_indexes_;
};

/**
 * @brief File-based table implementation
 */
class FileTable : public Table {
public:
    FileTable(const std::string& base_path, const TableSchema& schema)
        : base_path_(base_path)
        , schema_(schema) {
        table_path_ = base_path_ + "/" + schema_.table_name;
        fs::create_directories(table_path_ + "/partitions");
        LoadPartitions();
    }

    std::string GetName() const override {
        return schema_.table_name;
    }

    const TableSchema& GetSchema() const override {
        return schema_;
    }

    Status Append(const arrow::RecordBatch& batch) override {
        // Determine partition for this batch
        std::string partition_id = GetPartitionId(batch);

        // Get or create partition
        std::shared_ptr<Partition> partition;
        {
            std::lock_guard<std::mutex> lock(partitions_mutex_);
            auto it = partitions_.find(partition_id);
            if (it == partitions_.end()) {
                auto new_partition = std::make_shared<FilePartition>(
                    table_path_, partition_id, schema_);
                partitions_[partition_id] = new_partition;
                partition = new_partition;
            } else {
                partition = it->second;
            }
        }

        return partition->Append(batch);
    }

    Status Scan(const ScanSpec& spec, std::unique_ptr<QueryResult>* result) override {
        // Build partition metadata for query optimization
        std::vector<PartitionMetadata> partition_metadata;
        {
            std::lock_guard<std::mutex> lock(partitions_mutex_);
            for (const auto& [partition_id, partition] : partitions_) {
                PartitionMetadata metadata(partition_id);
                metadata.row_count = partition->GetRowCount();
                metadata.size_bytes = partition->GetSizeBytes();

                int64_t min_time, max_time;
                if (partition->GetTimeRange(&min_time, &max_time).ok()) {
                    metadata.min_timestamp = min_time;
                    metadata.max_timestamp = max_time;
                }

                partition_metadata.push_back(metadata);
            }
        }

        // Use query optimizer to select partitions
        QueryOptimizer optimizer;
        ScanSpec optimized_spec;
        std::vector<std::string> selected_partitions;

        auto optimize_status = optimizer.OptimizeScan(spec, partition_metadata,
                                                     &optimized_spec, &selected_partitions);
        if (!optimize_status.ok()) {
            // Fallback to scanning all partitions
            selected_partitions.clear();
            std::lock_guard<std::mutex> lock(partitions_mutex_);
            for (const auto& [id, _] : partitions_) {
                selected_partitions.push_back(id);
            }
        }

        // Scan selected partitions
        std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
        int64_t total_rows = 0;

        for (const auto& partition_id : selected_partitions) {
            std::shared_ptr<Partition> partition;
            {
                std::lock_guard<std::mutex> lock(partitions_mutex_);
                auto it = partitions_.find(partition_id);
                if (it != partitions_.end()) {
                    partition = it->second;
                } else {
                    continue;
                }
            }

            std::unique_ptr<QueryResult> partition_result;
            auto status = partition->Scan(optimized_spec, &partition_result);
            if (!status.ok()) {
                continue;  // Skip failed partitions
            }

            // Drain the query result into batches
            while (partition_result->HasNext()) {
                std::shared_ptr<arrow::RecordBatch> batch;
                auto batch_status = partition_result->Next(&batch);
                if (batch_status.ok() && batch && batch->num_rows() > 0) {
                    all_batches.push_back(batch);
                    total_rows += batch->num_rows();
                }
            }
        }

        if (all_batches.empty()) {
            auto empty_table_result = arrow::Table::MakeEmpty(schema_.arrow_schema);
            if (!empty_table_result.ok()) {
                return Status::FromArrowStatus(empty_table_result.status());
            }
            auto empty_table = empty_table_result.ValueUnsafe();
            return TableQueryResult::Create(empty_table, result);
        }

        // Concatenate all results
        auto table_result = arrow::Table::FromRecordBatches(all_batches);
        if (!table_result.ok()) {
            return Status::FromArrowStatus(table_result.status());
        }
        auto table = table_result.ValueUnsafe();

        return TableQueryResult::Create(table, result);
    }

    Status ListPartitions(std::vector<std::string>* partitions) const override {
        std::lock_guard<std::mutex> lock(partitions_mutex_);
        partitions->clear();
        for (const auto& [id, _] : partitions_) {
            partitions->push_back(id);
        }
        return Status::OK();
    }

    Status GetPartition(const std::string& partition_id,
                       std::shared_ptr<Partition>* partition) const override {
        std::lock_guard<std::mutex> lock(partitions_mutex_);
        auto it = partitions_.find(partition_id);
        if (it == partitions_.end()) {
            return Status::NotFound("Partition not found: " + partition_id);
        }
        *partition = it->second;
        return Status::OK();
    }

    Status GetStats(std::unordered_map<std::string, std::string>* stats) const override {
        std::lock_guard<std::mutex> lock(partitions_mutex_);
        int64_t total_rows = 0;
        int64_t total_size = 0;
        int partition_count = partitions_.size();

        for (const auto& [_, partition] : partitions_) {
            total_rows += partition->GetRowCount();
            total_size += partition->GetSizeBytes();
        }

        (*stats)["table_name"] = schema_.table_name;
        (*stats)["total_rows"] = std::to_string(total_rows);
        (*stats)["total_size_bytes"] = std::to_string(total_size);
        (*stats)["partition_count"] = std::to_string(partition_count);

        return Status::OK();
    }

private:
    std::string GetPartitionId(const arrow::RecordBatch& batch) {
        // For now, use current date as partition ID
        // TODO: Extract timestamp from batch and create proper partition ID
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d");
        return ss.str();
    }

    void LoadPartitions() {
        std::string partitions_dir = table_path_ + "/partitions";
        if (!fs::exists(partitions_dir)) {
            return;
        }

        for (const auto& entry : fs::directory_iterator(partitions_dir)) {
            if (entry.is_directory()) {
                std::string partition_id = entry.path().filename().string();
                auto partition = std::make_shared<FilePartition>(
                    table_path_, partition_id, schema_);
                partitions_[partition_id] = partition;
            }
        }
    }

    std::string base_path_;
    std::string table_path_;
    TableSchema schema_;
    mutable std::mutex partitions_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Partition>> partitions_;
};

/**
 * @brief File-based database implementation
 */
class FileDatabase : public Database {
public:
    explicit FileDatabase(const std::string& base_path)
        : base_path_(base_path) {
        fs::create_directories(base_path_);
        LoadTables();
    }

    Status CreateTable(const TableSchema& schema) override {
        std::lock_guard<std::mutex> lock(tables_mutex_);

        if (tables_.find(schema.table_name) != tables_.end()) {
            return Status::AlreadyExists("Table already exists: " + schema.table_name);
        }

        auto table = std::make_shared<FileTable>(base_path_, schema);
        tables_[schema.table_name] = table;

        // Save schema metadata
        SaveTableSchema(schema);

        return Status::OK();
    }

    Status DropTable(const std::string& table_name) override {
        std::lock_guard<std::mutex> lock(tables_mutex_);

        auto it = tables_.find(table_name);
        if (it == tables_.end()) {
            return Status::NotFound("Table not found: " + table_name);
        }

        // Remove table directory
        std::string table_path = base_path_ + "/" + table_name;
        try {
            fs::remove_all(table_path);
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Failed to remove table directory: ") + e.what());
        }

        tables_.erase(it);
        return Status::OK();
    }

    Status GetTable(const std::string& table_name,
                   std::shared_ptr<Table>* table) override {
        std::lock_guard<std::mutex> lock(tables_mutex_);

        auto it = tables_.find(table_name);
        if (it == tables_.end()) {
            return Status::NotFound("Table not found: " + table_name);
        }

        *table = it->second;
        return Status::OK();
    }

    Status ListTables(std::vector<std::string>* tables) const override {
        std::lock_guard<std::mutex> lock(tables_mutex_);

        tables->clear();
        for (const auto& [name, _] : tables_) {
            tables->push_back(name);
        }
        return Status::OK();
    }

    Status GetStats(std::unordered_map<std::string, std::string>* stats) const override {
        std::lock_guard<std::mutex> lock(tables_mutex_);

        (*stats)["database_path"] = base_path_;
        (*stats)["table_count"] = std::to_string(tables_.size());

        int64_t total_rows = 0;
        int64_t total_size = 0;
        for (const auto& [_, table] : tables_) {
            std::unordered_map<std::string, std::string> table_stats;
            table->GetStats(&table_stats);
            total_rows += std::stoll(table_stats["total_rows"]);
            total_size += std::stoll(table_stats["total_size_bytes"]);
        }

        (*stats)["total_rows"] = std::to_string(total_rows);
        (*stats)["total_size_bytes"] = std::to_string(total_size);

        return Status::OK();
    }

private:
    void SaveTableSchema(const TableSchema& schema) {
        // TODO: Save schema metadata to disk
        // For now, schemas are recreated on load
    }

    void LoadTables() {
        // TODO: Load table schemas from metadata
        // For now, tables are discovered by directory scanning
        if (!fs::exists(base_path_)) {
            return;
        }

        for (const auto& entry : fs::directory_iterator(base_path_)) {
            if (entry.is_directory()) {
                std::string table_name = entry.path().filename().string();

                // Skip special directories
                if (table_name == "." || table_name == "..") {
                    continue;
                }

                // TODO: Load schema from metadata
                // For now, create a basic schema
                auto field = arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO));
                auto arrow_schema = arrow::schema({field});
                TableSchema schema(table_name, arrow_schema);

                auto table = std::make_shared<FileTable>(base_path_, schema);
                tables_[table_name] = table;
            }
        }
    }

    std::string base_path_;
    mutable std::mutex tables_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};

// Factory functions
std::shared_ptr<Database> CreateDatabase(const std::string& base_path) {
    return std::make_shared<FileDatabase>(base_path);
}

std::shared_ptr<Table> CreateTable(const std::string& base_path,
                                  const TableSchema& schema) {
    return std::make_shared<FileTable>(base_path, schema);
}

} // namespace marble
