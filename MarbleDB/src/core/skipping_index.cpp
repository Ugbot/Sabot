#include "marble/skipping_index.h"
#include <algorithm>
#include <numeric>
#include <sstream>
#include <arrow/compute/api.h>
#include <arrow/table.h>
#include <nlohmann/json.hpp>

namespace marble {

//==============================================================================
// InMemorySkippingIndex Implementation
//==============================================================================

Status InMemorySkippingIndex::BuildFromTable(const std::shared_ptr<arrow::Table>& table,
                                           int64_t block_size_rows) {
    if (!table || table->num_rows() == 0) {
        return Status::InvalidArgument("Table is empty or null");
    }

    blocks_.clear();

    int64_t total_rows = table->num_rows();
    int64_t num_blocks = (total_rows + block_size_rows - 1) / block_size_rows;

    for (int64_t block_id = 0; block_id < num_blocks; ++block_id) {
        int64_t start_row = block_id * block_size_rows;
        int64_t end_row = std::min(start_row + block_size_rows, total_rows);

        BlockStats stats(block_id, start_row, end_row - start_row);

        // Compute statistics for this block
        auto status = ComputeBlockStats(table, start_row, end_row, &stats);
        if (!status.ok()) return status;

        blocks_.push_back(std::move(stats));
    }

    return Status::OK();
}

Status InMemorySkippingIndex::GetCandidateBlocks(const std::string& column_name,
                                               const std::string& op,
                                               const std::shared_ptr<arrow::Scalar>& value,
                                               std::vector<int64_t>* candidate_blocks) const {
    candidate_blocks->clear();

    for (const auto& block : blocks_) {
        if (CanBlockSatisfy(block, column_name, op, value)) {
            candidate_blocks->push_back(block.block_id);
        }
    }

    return Status::OK();
}

Status InMemorySkippingIndex::GetBlockStats(int64_t block_id, BlockStats* stats) const {
    auto it = std::find_if(blocks_.begin(), blocks_.end(),
                          [block_id](const BlockStats& b) { return b.block_id == block_id; });

    if (it == blocks_.end()) {
        return Status::NotFound("Block not found: " + std::to_string(block_id));
    }

    *stats = *it;
    return Status::OK();
}

std::vector<uint8_t> InMemorySkippingIndex::Serialize() const {
    nlohmann::json j;
    j["type"] = "InMemorySkippingIndex";
    j["num_blocks"] = blocks_.size();

    nlohmann::json blocks_json = nlohmann::json::array();
    for (const auto& block : blocks_) {
        nlohmann::json block_json;
        block_json["block_id"] = block.block_id;
        block_json["row_offset"] = block.row_offset;
        block_json["row_count"] = block.row_count;
        block_json["byte_offset"] = block.byte_offset;
        block_json["byte_count"] = block.byte_count;
        block_json["min_timestamp"] = block.min_timestamp;
        block_json["max_timestamp"] = block.max_timestamp;
        block_json["timestamp_count"] = block.timestamp_count;

        // Serialize min/max values (simplified - in practice would handle all types)
        for (const auto& [col, min_val] : block.min_values) {
            if (min_val && min_val->type->id() == arrow::Type::INT64) {
                block_json["min_" + col] = static_cast<const arrow::Int64Scalar&>(*min_val).value;
            }
        }
        for (const auto& [col, max_val] : block.max_values) {
            if (max_val && max_val->type->id() == arrow::Type::INT64) {
                block_json["max_" + col] = static_cast<const arrow::Int64Scalar&>(*max_val).value;
            }
        }

        blocks_json.push_back(block_json);
    }
    j["blocks"] = blocks_json;

    std::string json_str = j.dump();
    return std::vector<uint8_t>(json_str.begin(), json_str.end());
}

Status InMemorySkippingIndex::Deserialize(const std::vector<uint8_t>& data) {
    try {
        std::string json_str(data.begin(), data.end());
        nlohmann::json j = nlohmann::json::parse(json_str);

        blocks_.clear();

        for (const auto& block_json : j["blocks"]) {
            BlockStats stats;
            stats.block_id = block_json["block_id"];
            stats.row_offset = block_json["row_offset"];
            stats.row_count = block_json["row_count"];
            stats.byte_offset = block_json["byte_offset"];
            stats.byte_count = block_json["byte_count"];
            stats.min_timestamp = block_json["min_timestamp"];
            stats.max_timestamp = block_json["max_timestamp"];
            stats.timestamp_count = block_json["timestamp_count"];

            // Deserialize would need proper type handling in production
            blocks_.push_back(stats);
        }

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Failed to deserialize skipping index: " + std::string(e.what()));
    }
}

size_t InMemorySkippingIndex::MemoryUsage() const {
    size_t total = sizeof(InMemorySkippingIndex);
    for (const auto& block : blocks_) {
        total += sizeof(BlockStats);
        total += block.min_values.size() * sizeof(std::pair<std::string, std::shared_ptr<arrow::Scalar>>);
        total += block.max_values.size() * sizeof(std::pair<std::string, std::shared_ptr<arrow::Scalar>>);
    }
    return total;
}

Status InMemorySkippingIndex::ComputeBlockStats(const std::shared_ptr<arrow::Table>& table,
                                              int64_t start_row,
                                              int64_t end_row,
                                              BlockStats* stats) {
    auto schema = table->schema();

    for (int col_idx = 0; col_idx < schema->num_fields(); ++col_idx) {
        const auto& field = schema->field(col_idx);
        const auto& column = table->column(col_idx);

        // Slice the column to get only the rows for this block
        auto sliced_column = column->Slice(start_row, end_row - start_row);
        if (!sliced_column) continue;

        // Compute min/max for this column
        if (field->type()->id() == arrow::Type::INT64) {
            auto int64_array = std::static_pointer_cast<arrow::Int64Array>(sliced_column->chunk(0));

            if (int64_array->length() > 0) {
                int64_t min_val = *std::min_element(int64_array->raw_values(),
                                                   int64_array->raw_values() + int64_array->length());
                int64_t max_val = *std::max_element(int64_array->raw_values(),
                                                   int64_array->raw_values() + int64_array->length());

                stats->min_values[field->name()] = arrow::MakeScalar(min_val);
                stats->max_values[field->name()] = arrow::MakeScalar(max_val);
            }
        }
        // Add more type handling as needed (DOUBLE, STRING, TIMESTAMP, etc.)

        // Count nulls
        int64_t null_count = 0;
        for (int chunk_idx = 0; chunk_idx < sliced_column->num_chunks(); ++chunk_idx) {
            auto chunk = sliced_column->chunk(chunk_idx);
            for (int64_t row_in_chunk = 0; row_in_chunk < chunk->length(); ++row_in_chunk) {
                if (chunk->IsNull(row_in_chunk)) {
                    null_count++;
                }
            }
        }
        stats->null_counts[field->name()] = null_count;
    }

    // Handle timestamp column specially
    auto timestamp_field = schema->GetFieldByName("timestamp");
    if (timestamp_field) {
        auto timestamp_column = table->GetColumnByName("timestamp");
        if (timestamp_column) {
            auto sliced_timestamps = timestamp_column->Slice(start_row, end_row - start_row);
            if (sliced_timestamps && sliced_timestamps->length() > 0) {
                // Extract timestamp values (simplified)
                stats->timestamp_count = sliced_timestamps->length();
                // Would need to compute min/max timestamps in production
            }
        }
    }

    return Status::OK();
}

bool InMemorySkippingIndex::CanBlockSatisfy(const BlockStats& block,
                                          const std::string& column_name,
                                          const std::string& op,
                                          const std::shared_ptr<arrow::Scalar>& value) const {
    // Check if this block can possibly satisfy the predicate
    auto min_it = block.min_values.find(column_name);
    auto max_it = block.max_values.find(column_name);

    if (min_it == block.min_values.end() || max_it == block.max_values.end()) {
        // No statistics for this column, can't prune
        return true;
    }

    if (!min_it->second || !max_it->second || !value) {
        return true; // Can't compare, assume it might match
    }

    // Perform predicate evaluation based on min/max statistics
    // TODO: Implement proper Arrow scalar comparison for predicate pushdown
    // For now, return true (conservative approach - assume block might contain matches)
    if (op == "=" || op == "==" || op == ">" || op == ">=" || op == "<" || op == "<=") {
        return true; // Conservative: assume block might contain matching values
    }

    // For complex predicates, assume block might contain matching rows
    return true;
}

//==============================================================================
// GranularSkippingIndex Implementation
//==============================================================================

Status GranularSkippingIndex::BuildFromTable(const std::shared_ptr<arrow::Table>& table,
                                           int64_t block_size_rows) {
    // First build blocks using the parent class implementation
    // Call through the parent's implementation directly
    blocks_.clear();
    
    if (!table || table->num_rows() == 0) {
        return Status::InvalidArgument("Table is null or empty");
    }

    int64_t total_rows = table->num_rows();
    int64_t num_blocks = (total_rows + block_size_rows - 1) / block_size_rows;
    
    // Build block statistics (simplified)
    for (int64_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
        int64_t start_row = block_idx * block_size_rows;
        int64_t end_row = std::min(start_row + block_size_rows, total_rows);

        BlockStats block;
        block.block_id = block_idx;
        block.row_offset = start_row;
        block.row_count = end_row - start_row;
        blocks_.push_back(block);
    }

    // Then build granules within each block for finer granularity
    granules_.clear();

    for (const auto& block : blocks_) {
        std::vector<GranuleStats> block_granules;

        int64_t num_granules = (block.row_count + granule_size_rows_ - 1) / granule_size_rows_;

        for (int64_t granule_idx = 0; granule_idx < num_granules; ++granule_idx) {
            int64_t granule_start = block.row_offset + granule_idx * granule_size_rows_;
            int64_t granule_end = std::min(granule_start + granule_size_rows_,
                                         block.row_offset + block.row_count);

            GranuleStats granule;
            granule.granule_id = block.block_id * 1000 + granule_idx; // Unique ID
            granule.block_id = block.block_id;
            granule.row_offset = granule_start;
            granule.row_count = granule_end - granule_start;

            // Compute min/max for each column in this granule
            for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
                const auto& column = table->column(col_idx);
                const auto& field_name = table->schema()->field(col_idx)->name();

                auto sliced = column->Slice(granule_start, granule.row_count);
                if (sliced && sliced->length() > 0) {
                    // Compute min/max for this granule (simplified)
                    if (table->schema()->field(col_idx)->type()->id() == arrow::Type::INT64) {
                        auto int_array = std::static_pointer_cast<arrow::Int64Array>(sliced->chunk(0));
                        if (int_array->length() > 0) {
                            int64_t min_val = *std::min_element(int_array->raw_values(),
                                                               int_array->raw_values() + int_array->length());
                            int64_t max_val = *std::max_element(int_array->raw_values(),
                                                               int_array->raw_values() + int_array->length());

                            granule.min_values[field_name] = arrow::MakeScalar(min_val);
                            granule.max_values[field_name] = arrow::MakeScalar(max_val);
                        }
                    }
                }
            }

            block_granules.push_back(granule);
        }

        granules_[block.block_id] = std::move(block_granules);
    }

    return Status::OK();
}

Status GranularSkippingIndex::GetCandidateBlocks(const std::string& column_name,
                                               const std::string& op,
                                               const std::shared_ptr<arrow::Scalar>& value,
                                               std::vector<int64_t>* candidate_blocks) const {
    // For now, return all blocks as candidates (conservative approach)
    candidate_blocks->clear();
    for (const auto& block : blocks_) {
        candidate_blocks->push_back(block.block_id);
    }
    return Status::OK();
}

Status GranularSkippingIndex::GetBlockStats(int64_t block_id, BlockStats* stats) const {
    // Find the block with the given ID
    for (const auto& block : blocks_) {
        if (block.block_id == block_id) {
            *stats = block;
            return Status::OK();
        }
    }
    return Status::NotFound("Block not found");
}

std::vector<uint8_t> GranularSkippingIndex::Serialize() const {
    // Simplified serialization - would need to include granules in production
    // For now, return empty vector
    return std::vector<uint8_t>();
}

Status GranularSkippingIndex::Deserialize(const std::vector<uint8_t>& data) {
    // TODO: Implement granular deserialize
    return Status::NotImplemented("Granular index deserialization not implemented");
}

size_t GranularSkippingIndex::MemoryUsage() const {
    size_t base_usage = blocks_.size() * sizeof(BlockStats);

    // Add granule memory usage
    for (const auto& [block_id, granules] : granules_) {
        base_usage += sizeof(int64_t); // block_id key
        base_usage += sizeof(std::vector<GranuleStats>);
        for (const auto& granule : granules) {
            base_usage += sizeof(GranuleStats);
        }
    }

    return base_usage;
}

const std::vector<GranularSkippingIndex::GranuleStats>&
GranularSkippingIndex::GetGranulesForBlock(int64_t block_id) const {
    static const std::vector<GranuleStats> empty_granules;
    auto it = granules_.find(block_id);
    return it != granules_.end() ? it->second : empty_granules;
}

//==============================================================================
// TimeSeriesSkippingIndex Implementation
//==============================================================================

Status TimeSeriesSkippingIndex::BuildFromTable(const std::shared_ptr<arrow::Table>& table,
                                             int64_t block_size_rows) {
    // First build blocks
    auto status = InMemorySkippingIndex::BuildFromTable(table, block_size_rows);
    if (!status.ok()) return status;

    // Then build time-specific buckets
    time_buckets_.clear();

    // Identify time and value columns (simplified - assume first timestamp and numeric column)
    auto schema = table->schema();
    for (int i = 0; i < schema->num_fields(); ++i) {
        if (schema->field(i)->type()->id() == arrow::Type::TIMESTAMP) {
            time_column_ = schema->field(i)->name();
        } else if (schema->field(i)->type()->id() == arrow::Type::DOUBLE ||
                   schema->field(i)->type()->id() == arrow::Type::FLOAT ||
                   schema->field(i)->type()->id() == arrow::Type::INT64) {
            if (value_column_.empty()) {
                value_column_ = schema->field(i)->name();
            }
        }
    }

    // Create time buckets (e.g., hourly buckets)
    const int64_t bucket_duration_ms = 3600000; // 1 hour

    for (const auto& block : blocks_) {
        if (block.timestamp_count > 0) {
            TimeBucketStats bucket;
            bucket.bucket_id = block.block_id;
            bucket.start_timestamp = block.min_timestamp;
            bucket.end_timestamp = block.max_timestamp;
            bucket.row_count = block.row_count;

            // Compute value statistics for this time bucket
            if (!value_column_.empty()) {
                auto value_column = table->GetColumnByName(value_column_);
                if (value_column) {
                    auto sliced_values = value_column->Slice(block.row_offset, block.row_count);
                    if (sliced_values) {
                        // Compute statistics (simplified)
                        bucket.value_count = sliced_values->length();
                        // Would compute min/max/avg in production
                    }
                }
            }

            time_buckets_.push_back(bucket);
        }
    }

    return Status::OK();
}

Status TimeSeriesSkippingIndex::GetCandidateBlocks(const std::string& column_name,
                                                 const std::string& op,
                                                 const std::shared_ptr<arrow::Scalar>& value,
                                                 std::vector<int64_t>* candidate_blocks) const {
    // For time-based queries, use time buckets for optimization
    if (column_name == time_column_ && value && value->type->id() == arrow::Type::TIMESTAMP) {
        // Handle time range queries specially
        if (op == ">=" || op == ">" || op == "<=" || op == "<" || op == "=") {
            // Find time buckets that overlap with the query range
            // Simplified - would need proper timestamp comparison
            return InMemorySkippingIndex::GetCandidateBlocks(column_name, op, value, candidate_blocks);
        }
    }

    // Use base implementation for non-time queries
    return SkippingIndex::GetCandidateBlocks(column_name, op, value, candidate_blocks);
}

Status TimeSeriesSkippingIndex::GetBlockStats(int64_t block_id, BlockStats* stats) const {
    return InMemorySkippingIndex::GetBlockStats(block_id, stats);
}

std::vector<uint8_t> TimeSeriesSkippingIndex::Serialize() const {
    // Would need to include time buckets in serialization
    return InMemorySkippingIndex::Serialize();
}

Status TimeSeriesSkippingIndex::Deserialize(const std::vector<uint8_t>& data) {
    return InMemorySkippingIndex::Deserialize(data);
}

size_t TimeSeriesSkippingIndex::MemoryUsage() const {
    size_t base_usage = InMemorySkippingIndex::MemoryUsage();
    base_usage += time_buckets_.size() * sizeof(TimeBucketStats);
    return base_usage;
}

Status TimeSeriesSkippingIndex::GetTimeBuckets(int64_t start_time, int64_t end_time,
                                             std::vector<TimeBucketStats>* buckets) const {
    buckets->clear();

    for (const auto& bucket : time_buckets_) {
        // Check if bucket overlaps with query range
        if (bucket.end_timestamp >= start_time && bucket.start_timestamp <= end_time) {
            buckets->push_back(bucket);
        }
    }

    return Status::OK();
}

//==============================================================================
// SkippingIndexManager Implementation
//==============================================================================

Status SkippingIndexManager::CreateSkippingIndex(const std::string& table_name,
                                               const std::shared_ptr<arrow::Table>& table,
                                               SkippingIndex** index) {
    // Determine which type of index to create based on table characteristics
    auto schema = table->schema();

    // Check if it looks like time series data
    bool has_timestamp = false;
    bool has_numeric_values = false;

    for (int i = 0; i < schema->num_fields(); ++i) {
        if (schema->field(i)->type()->id() == arrow::Type::TIMESTAMP) {
            has_timestamp = true;
        } else if (schema->field(i)->type()->id() == arrow::Type::DOUBLE ||
                   schema->field(i)->type()->id() == arrow::Type::FLOAT ||
                   schema->field(i)->type()->id() == arrow::Type::INT64) {
            has_numeric_values = true;
        }
    }

    std::unique_ptr<SkippingIndex> new_index;

    if (has_timestamp && has_numeric_values) {
        // Create time series optimized index
        new_index = CreateTimeSeriesSkippingIndex();
    } else if (table->num_rows() > 100000) {
        // Large tables get granular indexes
        new_index = CreateGranularSkippingIndex();
    } else {
        // Default to in-memory index
        new_index = CreateInMemorySkippingIndex();
    }

    // Build the index
    auto status = new_index->BuildFromTable(table);
    if (!status.ok()) return status;

    // Store it
    indexes_[table_name] = std::move(new_index);

    if (index) {
        *index = indexes_[table_name].get();
    }

    return Status::OK();
}

Status SkippingIndexManager::GetSkippingIndex(const std::string& table_name,
                                            SkippingIndex** index) const {
    auto it = indexes_.find(table_name);
    if (it == indexes_.end()) {
        return Status::NotFound("No skipping index for table: " + table_name);
    }

    *index = it->second.get();
    return Status::OK();
}

Status SkippingIndexManager::RemoveSkippingIndex(const std::string& table_name) {
    auto it = indexes_.find(table_name);
    if (it == indexes_.end()) {
        return Status::NotFound("No skipping index for table: " + table_name);
    }

    indexes_.erase(it);
    return Status::OK();
}

std::vector<std::string> SkippingIndexManager::ListIndexedTables() const {
    std::vector<std::string> tables;
    tables.reserve(indexes_.size());

    for (const auto& pair : indexes_) {
        tables.push_back(pair.first);
    }

    return tables;
}

size_t SkippingIndexManager::TotalMemoryUsage() const {
    size_t total = 0;
    for (const auto& pair : indexes_) {
        total += pair.second->MemoryUsage();
    }
    return total;
}

Status SkippingIndexManager::OptimizeQueryWithSkipping(const std::string& table_name,
                                                     const std::string& column_name,
                                                     const std::string& op,
                                                     const std::shared_ptr<arrow::Scalar>& value,
                                                     std::vector<int64_t>* candidate_blocks) {
    SkippingIndex* index = nullptr;
    auto status = GetSkippingIndex(table_name, &index);
    if (!status.ok()) {
        // No index available, return all blocks (no optimization)
        candidate_blocks->clear();
        return Status::OK();
    }

    return index->GetCandidateBlocks(column_name, op, value, candidate_blocks);
}

//==============================================================================
// Factory Functions
//==============================================================================

std::unique_ptr<SkippingIndex> CreateInMemorySkippingIndex() {
    return std::make_unique<InMemorySkippingIndex>();
}

std::unique_ptr<SkippingIndex> CreateGranularSkippingIndex() {
    return std::make_unique<GranularSkippingIndex>();
}

std::unique_ptr<SkippingIndex> CreateTimeSeriesSkippingIndex() {
    return std::make_unique<TimeSeriesSkippingIndex>();
}

std::unique_ptr<SkippingIndexManager> CreateSkippingIndexManager() {
    return std::make_unique<SkippingIndexManager>();
}

} // namespace marble
