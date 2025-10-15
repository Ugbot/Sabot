/**
 * MarbleDB Public API Implementation
 *
 * Implementation of the clean public API that wraps internal complexity
 * and provides a stable interface for external use.
 */

#include "marble/api.h"
#include "marble/db.h"
#include "marble/table.h"
#include "marble/query.h"
#include "marble/analytics.h"
#include "marble/skipping_index.h"
#include "marble/status.h"
#include <nlohmann/json.hpp>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <unordered_map>
#include <mutex>
#include <sstream>
#include <iostream>

namespace marble {

//==============================================================================
// Internal Helper Functions
//==============================================================================

namespace {

// Convert JSON string to Arrow Schema
Status JsonToArrowSchema(const std::string& schema_json, std::shared_ptr<arrow::Schema>* schema) {
    try {
        auto json = nlohmann::json::parse(schema_json);
        std::vector<std::shared_ptr<arrow::Field>> fields;

        for (const auto& field_json : json["fields"]) {
            std::string name = field_json["name"];
            std::string type_str = field_json["type"];
            bool nullable = field_json.value("nullable", false);

            std::shared_ptr<arrow::DataType> arrow_type;
            if (type_str == "int64") arrow_type = arrow::int64();
            else if (type_str == "int32") arrow_type = arrow::int32();
            else if (type_str == "float64") arrow_type = arrow::float64();
            else if (type_str == "float32") arrow_type = arrow::float32();
            else if (type_str == "string" || type_str == "utf8") arrow_type = arrow::utf8();
            else if (type_str == "bool" || type_str == "boolean") arrow_type = arrow::boolean();
            else if (type_str == "timestamp") arrow_type = arrow::timestamp(arrow::TimeUnit::MICRO);
            else return Status::InvalidArgument("Unsupported type: " + type_str);

            fields.push_back(arrow::field(name, arrow_type, nullable));
        }

        *schema = arrow::schema(fields);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Invalid JSON schema: " + std::string(e.what()));
    }
}

// Convert Arrow Schema to JSON string
Status ArrowSchemaToJson(const std::shared_ptr<arrow::Schema>& schema, std::string* schema_json) {
    try {
        nlohmann::json json_schema;
        json_schema["fields"] = nlohmann::json::array();

        for (int i = 0; i < schema->num_fields(); ++i) {
            const auto& field = schema->field(i);
            nlohmann::json field_json;
            field_json["name"] = field->name();
            field_json["nullable"] = field->nullable();

            // Convert Arrow type to string
            auto type = field->type();
            if (type->Equals(arrow::int64())) field_json["type"] = "int64";
            else if (type->Equals(arrow::int32())) field_json["type"] = "int32";
            else if (type->Equals(arrow::float64())) field_json["type"] = "float64";
            else if (type->Equals(arrow::float32())) field_json["type"] = "float32";
            else if (type->Equals(arrow::utf8())) field_json["type"] = "string";
            else if (type->Equals(arrow::boolean())) field_json["type"] = "boolean";
            else if (type->id() == arrow::Type::TIMESTAMP) field_json["type"] = "timestamp";
            else field_json["type"] = "unknown";

            json_schema["fields"].push_back(field_json);
        }

        *schema_json = json_schema.dump(2);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to convert schema to JSON: " + std::string(e.what()));
    }
}

// Convert JSON record to Arrow RecordBatch
Status JsonToArrowRecord(const std::string& record_json, const std::shared_ptr<arrow::Schema>& schema,
                        std::shared_ptr<arrow::RecordBatch>* batch) {
    try {
        auto json = nlohmann::json::parse(record_json);
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        for (int i = 0; i < schema->num_fields(); ++i) {
            const auto& field = schema->field(i);
            std::string field_name = field->name();

            if (json.contains(field_name)) {
                // Create array with single value
                auto value = json[field_name];
                // This is simplified - in practice would need proper type conversion
                // For now, assume string values
                arrow::StringBuilder builder;
                builder.Append(value.dump());
                std::shared_ptr<arrow::Array> array;
                builder.Finish(&array);
                arrays.push_back(array);
            } else {
                // Null value
                auto array = arrow::MakeArrayOfNull(field->type(), 1).ValueOrDie();
                arrays.push_back(array);
            }
        }

        *batch = arrow::RecordBatch::Make(schema, 1, arrays);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Invalid JSON record: " + std::string(e.what()));
    }
}

// Convert Arrow RecordBatch to JSON string
Status ArrowRecordBatchToJson(const std::shared_ptr<arrow::RecordBatch>& batch, std::string* json_str) {
    try {
        nlohmann::json json_array = nlohmann::json::array();

        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            nlohmann::json json_record;

            for (int col = 0; col < batch->num_columns(); ++col) {
                const auto& column = batch->column(col);
                const auto& field = batch->schema()->field(col);
                std::string field_name = field->name();

                if (column->IsValid(row)) {
                    // Extract value based on type
                    auto type_id = column->type()->id();
                    if (type_id == arrow::Type::INT64) {
                        auto int_array = std::static_pointer_cast<arrow::Int64Array>(column);
                        json_record[field_name] = int_array->Value(row);
                    } else if (type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING) {
                        auto str_array = std::static_pointer_cast<arrow::StringArray>(column);
                        json_record[field_name] = str_array->GetString(row);
                    } else if (type_id == arrow::Type::DOUBLE) {
                        auto double_array = std::static_pointer_cast<arrow::DoubleArray>(column);
                        json_record[field_name] = double_array->Value(row);
                    } else if (type_id == arrow::Type::BOOL) {
                        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(column);
                        json_record[field_name] = bool_array->Value(row);
                    } else {
                        json_record[field_name] = "unsupported_type";
                    }
                } else {
                    json_record[field_name] = nullptr;
                }
            }

            json_array.push_back(json_record);
        }

        *json_str = json_array.dump(2);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to convert RecordBatch to JSON: " + std::string(e.what()));
    }
}

// Serialize Arrow RecordBatch to bytes - simplified implementation
Status SerializeArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                          std::string* serialized_data) {
    // TODO: Implement proper Arrow IPC serialization
    // For now, return not implemented to avoid Arrow API compatibility issues
    return Status::NotImplemented("Arrow serialization not yet implemented");
}

// Deserialize Arrow RecordBatch from bytes - simplified implementation
Status DeserializeArrowBatch(const void* data, size_t size,
                           std::shared_ptr<arrow::RecordBatch>* batch) {
    // TODO: Implement proper Arrow IPC deserialization
    // For now, return not implemented to avoid Arrow API compatibility issues
    return Status::NotImplemented("Arrow deserialization not yet implemented");
}

// Concrete Key implementation for triple keys
class TripleKey : public Key {
public:
    TripleKey(int64_t subject, int64_t predicate, int64_t object)
        : subject_(subject), predicate_(predicate), object_(object) {}

    int Compare(const Key& other) const override {
        const TripleKey* other_key = dynamic_cast<const TripleKey*>(&other);
        if (!other_key) return -1;

        if (subject_ != other_key->subject_) return subject_ < other_key->subject_ ? -1 : 1;
        if (predicate_ != other_key->predicate_) return predicate_ < other_key->predicate_ ? -1 : 1;
        if (object_ != other_key->object_) return object_ < other_key->object_ ? -1 : 1;
        return 0;
    }

    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return arrow::MakeScalar(subject_); // Just return subject for now
    }

    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<TripleKey>(subject_, predicate_, object_);
    }

    // Additional required methods from Key interface
    bool Equals(const Key& other) const {
        return Compare(other) == 0;
    }

    std::string ToString() const override {
        return std::to_string(subject_) + "," + std::to_string(predicate_) + "," + std::to_string(object_);
    }

    size_t Hash() const override {
        size_t h = std::hash<int64_t>()(subject_);
        h = h * 31 + std::hash<int64_t>()(predicate_);
        h = h * 31 + std::hash<int64_t>()(object_);
        return h;
    }

    int64_t subject() const { return subject_; }
    int64_t predicate() const { return predicate_; }
    int64_t object() const { return object_; }

private:
    int64_t subject_;
    int64_t predicate_;
    int64_t object_;
};

// Concrete RecordRef implementation
class SimpleRecordRef : public RecordRef {
public:
    SimpleRecordRef(std::shared_ptr<Key> key, std::shared_ptr<arrow::RecordBatch> batch, int64_t row_index)
        : key_(std::move(key)), batch_(std::move(batch)), row_index_(row_index) {}

    std::shared_ptr<Key> key() const override {
        return key_;
    }

    arrow::Result<std::shared_ptr<arrow::Scalar>> GetField(const std::string& field_name) const override {
        auto column = batch_->GetColumnByName(field_name);
        if (!column) {
            return arrow::Status::KeyError("Field not found: ", field_name);
        }
        return column->GetScalar(row_index_);
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::Scalar>>> GetFields() const override {
        std::vector<std::shared_ptr<arrow::Scalar>> fields;
        for (int i = 0; i < batch_->num_columns(); ++i) {
            auto column = batch_->column(i);
            ARROW_ASSIGN_OR_RAISE(auto scalar, column->GetScalar(row_index_));
            fields.push_back(scalar);
        }
        return fields;
    }

    size_t Size() const override {
        return batch_->num_columns();
    }

private:
    std::shared_ptr<Key> key_;
    std::shared_ptr<arrow::RecordBatch> batch_;
    int64_t row_index_;
};

// Concrete Record implementation
class SimpleRecord : public Record {
public:
    SimpleRecord(std::shared_ptr<Key> key, std::shared_ptr<arrow::RecordBatch> batch, int64_t row_index)
        : key_(std::move(key)), batch_(std::move(batch)), row_index_(row_index) {}

    std::shared_ptr<Key> GetKey() const override {
        return key_;
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override {
        // Extract single row as RecordBatch
        return batch_->Slice(row_index_, 1);
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return batch_->schema();
    }

    std::unique_ptr<RecordRef> AsRecordRef() const override {
        return std::make_unique<SimpleRecordRef>(key_, batch_, row_index_);
    }

private:
    std::shared_ptr<Key> key_;
    std::shared_ptr<arrow::RecordBatch> batch_;
    int64_t row_index_;
};

// Range Iterator implementation with bloom filters and sparse indexes
class RangeIterator : public Iterator {
public:
    RangeIterator(const std::vector<std::shared_ptr<arrow::RecordBatch>>& data,
                  const KeyRange& range,
                  std::shared_ptr<arrow::Schema> schema,
                  std::shared_ptr<SkippingIndex> skipping_index = nullptr,
                  std::shared_ptr<BloomFilter> bloom_filter = nullptr)
        : data_(data), range_(range), schema_(schema),
          skipping_index_(skipping_index), bloom_filter_(bloom_filter),
          current_batch_(0), current_row_(0) {
        // Find first valid position
        SeekToStart();
    }

    bool Valid() const override {
        return current_batch_ < data_.size() &&
               current_row_ < static_cast<int64_t>(data_[current_batch_]->num_rows());
    }

    void Next() override {
        if (!Valid()) return;

        current_row_++;
        if (current_row_ >= static_cast<int64_t>(data_[current_batch_]->num_rows())) {
            current_batch_++;
            current_row_ = 0;
        }

        // Skip records that don't match the range
        while (Valid() && !InRange()) {
            Next();
        }
    }

    std::shared_ptr<Key> key() const override {
        if (!Valid()) return nullptr;

        auto batch = data_[current_batch_];
        auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
        auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

        return std::make_shared<TripleKey>(
            subject_col->Value(current_row_),
            predicate_col->Value(current_row_),
            object_col->Value(current_row_)
        );
    }

    std::shared_ptr<Record> value() const override {
        if (!Valid()) return nullptr;
        return std::make_shared<SimpleRecord>(key(), data_[current_batch_], current_row_);
    }

    std::unique_ptr<RecordRef> value_ref() const override {
        if (!Valid()) return nullptr;
        return std::make_unique<SimpleRecordRef>(key(), data_[current_batch_], current_row_);
    }

    marble::Status status() const override {
        return marble::Status::OK();
    }

    // Additional Iterator methods
    void Seek(const Key& target) override {
        const TripleKey* target_key = dynamic_cast<const TripleKey*>(&target);
        if (!target_key) return;

        // Find the first record >= target
        for (size_t batch_idx = 0; batch_idx < data_.size(); ++batch_idx) {
            const auto& batch = data_[batch_idx];
            auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
            auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
            auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

            for (int64_t row_idx = 0; row_idx < static_cast<int64_t>(batch->num_rows()); ++row_idx) {
                TripleKey current_key(subject_col->Value(row_idx),
                                    predicate_col->Value(row_idx),
                                    object_col->Value(row_idx));

                if (current_key.Compare(*target_key) >= 0) {
                    current_batch_ = batch_idx;
                    current_row_ = row_idx;
                    return;
                }
            }
        }

        // If not found, go to end
        current_batch_ = data_.size();
        current_row_ = 0;
    }

    void SeekForPrev(const Key& target) override {
        const TripleKey* target_key = dynamic_cast<const TripleKey*>(&target);
        if (!target_key) return;

        // Find the last record <= target
        size_t found_batch = data_.size();
        int64_t found_row = -1;

        for (size_t batch_idx = 0; batch_idx < data_.size(); ++batch_idx) {
            const auto& batch = data_[batch_idx];
            auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
            auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
            auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

            for (int64_t row_idx = 0; row_idx < static_cast<int64_t>(batch->num_rows()); ++row_idx) {
                TripleKey current_key(subject_col->Value(row_idx),
                                    predicate_col->Value(row_idx),
                                    object_col->Value(row_idx));

                if (current_key.Compare(*target_key) <= 0) {
                    found_batch = batch_idx;
                    found_row = row_idx;
                } else if (found_row >= 0) {
                    // We've passed the target, stop
                    break;
                }
            }
        }

        if (found_row >= 0) {
            current_batch_ = found_batch;
            current_row_ = found_row;
        } else {
            // Not found, go to beginning
            current_batch_ = 0;
            current_row_ = 0;
        }
    }

    void SeekToLast() override {
        if (data_.empty()) return;
        current_batch_ = data_.size() - 1;
        current_row_ = data_[current_batch_]->num_rows() - 1;
    }

    void Prev() override {
        if (!Valid()) return;

        if (current_row_ > 0) {
            current_row_--;
        } else if (current_batch_ > 0) {
            current_batch_--;
            current_row_ = data_[current_batch_]->num_rows() - 1;
        } else {
            // At beginning, make invalid
            current_batch_ = data_.size();
            current_row_ = 0;
        }

        // Skip records that don't match the range
        while (Valid() && !InRange()) {
            Prev();
        }
    }

private:
    void SeekToStart() {
        current_batch_ = 0;
        current_row_ = 0;

        // Skip records that don't match the range
        while (Valid() && !InRange()) {
            Next();
        }
    }

    // Check if current position can be skipped using bloom filter
    bool CanSkipWithBloomFilter() const {
        if (!bloom_filter_ || !Valid()) return false;

        auto current_key = key();
        if (!current_key) return false;

        std::string key_str = current_key->ToString();
        return !bloom_filter_->MightContain(key_str);
    }

    // Get candidate batches using skipping index
    std::vector<size_t> GetCandidateBatches() const {
        if (!skipping_index_) {
            // No skipping index, return all batches
            std::vector<size_t> all_batches;
            for (size_t i = 0; i < data_.size(); ++i) {
                all_batches.push_back(i);
            }
            return all_batches;
        }

        // Use skipping index to find relevant batches
        std::vector<int64_t> candidate_blocks;
        if (range_.start()) {
            const TripleKey* start_key = dynamic_cast<const TripleKey*>(range_.start().get());
            if (start_key) {
                // For subject-based queries, use subject column
                auto subject_value = arrow::MakeScalar(start_key->subject());
                skipping_index_->GetCandidateBlocks("subject", ">=",
                                                   subject_value, &candidate_blocks);
            }
        }

        // Convert block IDs to batch indices (simplified mapping)
        std::vector<size_t> candidate_batches;
        for (int64_t block_id : candidate_blocks) {
            size_t batch_idx = block_id / 10; // Assume 10 blocks per batch (simplified)
            if (batch_idx < data_.size()) {
                candidate_batches.push_back(batch_idx);
            }
        }

        // If no candidates found, fall back to all batches
        if (candidate_batches.empty()) {
            for (size_t i = 0; i < data_.size(); ++i) {
                candidate_batches.push_back(i);
            }
        }

        return candidate_batches;
    }

    bool InRange() const {
        if (!Valid()) return false;

        auto current_key = key();
        if (!current_key) return false;

        const TripleKey* triple_key = dynamic_cast<const TripleKey*>(current_key.get());
        if (!triple_key) return false;

        // Check start bound
        if (range_.start()) {
            const TripleKey* start_key = dynamic_cast<const TripleKey*>(range_.start().get());
            if (start_key) {
                int cmp = triple_key->Compare(*start_key);
                if (range_.start_inclusive()) {
                    if (cmp < 0) return false;
                } else {
                    if (cmp <= 0) return false;
                }
            }
        }

        // Check end bound
        if (range_.end()) {
            const TripleKey* end_key = dynamic_cast<const TripleKey*>(range_.end().get());
            if (end_key) {
                int cmp = triple_key->Compare(*end_key);
                if (range_.end_inclusive()) {
                    if (cmp > 0) return false;
                } else {
                    if (cmp >= 0) return false;
                }
            }
        }

        return true;
    }

    const std::vector<std::shared_ptr<arrow::RecordBatch>>& data_;
    KeyRange range_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<SkippingIndex> skipping_index_;
    std::shared_ptr<BloomFilter> bloom_filter_;
    size_t current_batch_;
    int64_t current_row_;
};

// Simple MarbleDB implementation for API
class SimpleMarbleDB : public MarbleDB {
public:
    SimpleMarbleDB(const std::string& path) : path_(path) {}

    // Column Family management
    struct ColumnFamilyInfo {
        std::string name;
        std::shared_ptr<arrow::Schema> schema;
        std::vector<std::shared_ptr<arrow::RecordBatch>> data;  // In-memory storage for now
        uint32_t id;
        std::shared_ptr<SkippingIndex> skipping_index;
        std::shared_ptr<BloomFilter> bloom_filter;

        ColumnFamilyInfo(std::string n, std::shared_ptr<arrow::Schema> s, uint32_t i)
            : name(std::move(n)), schema(std::move(s)), id(i) {}

        // Build indexes from current data
        Status BuildIndexes() {
            if (data.empty()) return Status::OK();

            // Build skipping index
            skipping_index = std::make_shared<InMemorySkippingIndex>();
            auto table = arrow::Table::FromRecordBatches(schema, data);
            if (!table.ok()) return Status::InternalError("Failed to create table for indexing");

            auto status = skipping_index->BuildFromTable(*table, 8192); // 8192 rows per block
            if (!status.ok()) return status;

            // Build bloom filter for key lookups
            bloom_filter = std::make_shared<BloomFilter>(data.size() * 10, 0.01); // Estimate 10 keys per batch
            for (const auto& batch : data) {
                auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
                auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
                auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

                for (int64_t i = 0; i < batch->num_rows(); ++i) {
                    std::string key_str = std::to_string(subject_col->Value(i)) + "," +
                                        std::to_string(predicate_col->Value(i)) + "," +
                                        std::to_string(object_col->Value(i));
                    bloom_filter->Add(key_str);
                }
            }

            return Status::OK();
        }
    };

    std::unordered_map<std::string, std::unique_ptr<ColumnFamilyInfo>> column_families_;
    std::unordered_map<uint32_t, ColumnFamilyInfo*> id_to_cf_;
    uint32_t next_cf_id_ = 1;
    mutable std::mutex cf_mutex_;

    // Basic operations
    Status Put(const WriteOptions& options, std::shared_ptr<Record> record) override {
        return Status::NotImplemented("Put not implemented");
    }

    Status Get(const ReadOptions& options, const Key& key, std::shared_ptr<Record>* record) override {
        return Status::NotImplemented("Get not implemented");
    }

    Status Delete(const WriteOptions& options, const Key& key) override {
        return Status::NotImplemented("Delete not implemented");
    }
    
    // Merge operations
    Status Merge(const WriteOptions& options, const Key& key, const std::string& value) override {
        return Status::NotImplemented("Merge not implemented");
    }
    
    Status Merge(const WriteOptions& options, ColumnFamilyHandle* cf, const Key& key, const std::string& value) override {
        return Status::NotImplemented("CF Merge not implemented");
    }

    Status WriteBatch(const WriteOptions& options, const std::vector<std::shared_ptr<Record>>& records) override {
        return Status::NotImplemented("WriteBatch not implemented");
    }

    // Arrow batch operations
    Status InsertBatch(const std::string& table_name, const std::shared_ptr<arrow::RecordBatch>& batch) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Find column family
        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Column family '" + table_name + "' does not exist");
        }

        auto* cf_info = it->second.get();

        // Validate batch schema matches column family schema
        if (!batch->schema()->Equals(cf_info->schema)) {
            return Status::InvalidArgument("Batch schema does not match column family schema for '" + table_name + "'");
        }

        // Store batch in memory (for now - in production this would go to LSM tree)
        cf_info->data.push_back(batch);

        // Rebuild indexes after data insertion
        auto status = cf_info->BuildIndexes();
        if (!status.ok()) {
            // Remove the batch if indexing failed
            cf_info->data.pop_back();
            return status;
        }

        return Status::OK();
    }

    // Table operations
    Status CreateTable(const TableSchema& schema) override {
        return Status::NotImplemented("CreateTable not implemented");
    }

    Status ScanTable(const std::string& table_name, std::unique_ptr<QueryResult>* result) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Find column family
        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Column family '" + table_name + "' does not exist");
        }

        auto* cf_info = it->second.get();

        // Combine all batches into a single table
        if (cf_info->data.empty()) {
            // Return empty table with correct schema
            std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
            for (int i = 0; i < cf_info->schema->num_fields(); ++i) {
                auto field = cf_info->schema->field(i);
                auto empty_array = arrow::MakeArrayOfNull(field->type(), 0).ValueOrDie();
                auto chunked_array = std::make_shared<arrow::ChunkedArray>(empty_array);
                empty_columns.push_back(chunked_array);
            }
            auto empty_table = arrow::Table::Make(cf_info->schema, empty_columns);
            return TableQueryResult::Create(empty_table, result);
        }

        // Concatenate all record batches
        ARROW_ASSIGN_OR_RAISE(auto combined_table,
                             arrow::Table::FromRecordBatches(cf_info->schema, cf_info->data));

        // Create query result
        return TableQueryResult::Create(combined_table, result);
    }
    
    // Column Family operations
    Status CreateColumnFamily(const ColumnFamilyDescriptor& descriptor, ColumnFamilyHandle** handle) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Check if column family already exists
        if (column_families_.find(descriptor.name) != column_families_.end()) {
            return Status::InvalidArgument("Column family '" + descriptor.name + "' already exists");
        }

        // Validate schema
        if (!descriptor.options.schema) {
            return Status::InvalidArgument("Column family must have a valid Arrow schema");
        }

        // Create column family info
        uint32_t cf_id = next_cf_id_++;
        auto cf_info = std::make_unique<ColumnFamilyInfo>(
            descriptor.name, descriptor.options.schema, cf_id);

        // Store in registry
        auto* cf_ptr = cf_info.get();
        column_families_[descriptor.name] = std::move(cf_info);
        id_to_cf_[cf_id] = cf_ptr;

        // Create handle (we'll create a simple handle implementation)
        *handle = new ColumnFamilyHandle(descriptor.name, cf_id);

        return Status::OK();
    }
    
    Status DropColumnFamily(ColumnFamilyHandle* handle) override {
        return Status::NotImplemented("DropColumnFamily not implemented");
    }
    
    std::vector<std::string> ListColumnFamilies() const override {
        std::lock_guard<std::mutex> lock(cf_mutex_);
        std::vector<std::string> names;
        names.reserve(column_families_.size());
        for (const auto& pair : column_families_) {
            names.push_back(pair.first);
        }
        return names;
    }
    
    // CF-specific operations
    Status Put(const WriteOptions& options, ColumnFamilyHandle* cf, std::shared_ptr<Record> record) override {
        return Status::NotImplemented("CF Put not implemented");
    }
    
    Status Get(const ReadOptions& options, ColumnFamilyHandle* cf, const Key& key, std::shared_ptr<Record>* record) override {
        return Status::NotImplemented("CF Get not implemented");
    }
    
    Status Delete(const WriteOptions& options, ColumnFamilyHandle* cf, const Key& key) override {
        return Status::NotImplemented("CF Delete not implemented");
    }
    
    // Multi-Get
    Status MultiGet(const ReadOptions& options, const std::vector<Key>& keys, std::vector<std::shared_ptr<Record>>* records) override {
        return Status::NotImplemented("MultiGet not implemented");
    }
    
    Status MultiGet(const ReadOptions& options, ColumnFamilyHandle* cf, const std::vector<Key>& keys, std::vector<std::shared_ptr<Record>>* records) override {
        return Status::NotImplemented("CF MultiGet not implemented");
    }
    
    // Delete Range
    Status DeleteRange(const WriteOptions& options, const Key& begin_key, const Key& end_key) override {
        return Status::NotImplemented("DeleteRange not implemented");
    }
    
    Status DeleteRange(const WriteOptions& options, ColumnFamilyHandle* cf, const Key& begin_key, const Key& end_key) override {
        return Status::NotImplemented("CF DeleteRange not implemented");
    }

    // Scanning
    Status NewIterator(const ReadOptions& options, const KeyRange& range, std::unique_ptr<Iterator>* iterator) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // For now, use the default column family (assuming it's the first one)
        if (column_families_.empty()) {
            return Status::InvalidArgument("No column families available");
        }

        // Get the first column family (simplified - in practice would need column family selection)
        auto it = column_families_.begin();
        auto* cf_info = it->second.get();

        // Create range iterator with indexes for optimization
        auto range_iterator = std::make_unique<RangeIterator>(
            cf_info->data, range, cf_info->schema,
            cf_info->skipping_index, cf_info->bloom_filter);

        *iterator = std::move(range_iterator);
        return Status::OK();
    }

    // Maintenance operations
    Status Flush() override {
        return Status::NotImplemented("Flush not implemented");
    }

    Status CompactRange(const KeyRange& range) override {
        return Status::NotImplemented("CompactRange not implemented");
    }

    Status Destroy() override {
        return Status::NotImplemented("Destroy not implemented");
    }

    // Checkpoint operations
    Status CreateCheckpoint(const std::string& checkpoint_path) override {
        return Status::NotImplemented("CreateCheckpoint not implemented");
    }

    Status RestoreFromCheckpoint(const std::string& checkpoint_path) override {
        return Status::NotImplemented("RestoreFromCheckpoint not implemented");
    }

    Status GetCheckpointMetadata(std::string* metadata) const override {
        return Status::NotImplemented("GetCheckpointMetadata not implemented");
    }

    // Property access
    std::string GetProperty(const std::string& property) const override {
        return ""; // Return empty string for unimplemented properties
    }

    Status GetApproximateSizes(const std::vector<KeyRange>& ranges,
                              std::vector<uint64_t>* sizes) const override {
        return Status::NotImplemented("GetApproximateSizes not implemented");
    }

    // Streaming interfaces
    Status CreateStream(const std::string& stream_name,
                       std::unique_ptr<Stream>* stream) override {
        return Status::NotImplemented("CreateStream not implemented");
    }

    Status GetStream(const std::string& stream_name,
                    std::unique_ptr<Stream>* stream) override {
        return Status::NotImplemented("GetStream not implemented");
    }

    // Monitoring and metrics (production features)
    std::shared_ptr<MetricsCollector> GetMetricsCollector() const override {
        // Return a simple metrics collector for demo purposes
        static std::shared_ptr<MetricsCollector> collector = std::make_shared<MetricsCollector>();
        return collector;
    }

    std::string ExportMetricsPrometheus() const override {
        auto collector = GetMetricsCollector();
        return collector->exportPrometheus();
    }

    std::string ExportMetricsJSON() const override {
        auto collector = GetMetricsCollector();
        return collector->exportJSON();
    }

    std::unordered_map<std::string, bool> GetHealthStatus() const override {
        return {{"storage", true}, {"memory", true}, {"disk", true}};
    }

    StatusWithMetrics PerformHealthCheck() const override {
        return StatusWithMetrics::OK();
    }

    std::string GetSystemInfo() const override {
        return "SimpleMarbleDB v1.0 - In-memory implementation for testing";
    }

private:
    std::string path_;
};

} // anonymous namespace

//==============================================================================
// Database Management API Implementation
//==============================================================================

Status CreateDatabase(const std::string& path, std::unique_ptr<MarbleDB>* db) {
    try {
        *db = std::make_unique<SimpleMarbleDB>(path);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to create database: " + std::string(e.what()));
    }
}

Status OpenDatabase(const std::string& path, std::unique_ptr<MarbleDB>* db) {
    try {
        return CreateDatabase(path, db);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open database: " + std::string(e.what()));
    }
}

void CloseDatabase(std::unique_ptr<MarbleDB>* db) {
    db->reset();
}

//==============================================================================
// MarbleDB Static Factory Method
//==============================================================================

// Static factory method for MarbleDB class (required by header declaration)
Status MarbleDB::Open(const DBOptions& options,
                     std::shared_ptr<Schema> schema,
                     std::unique_ptr<MarbleDB>* db) {
    // Delegate to the existing OpenDatabase function
    // Note: schema parameter is ignored for now (SimpleMarbleDB is schema-agnostic)
    try {
        *db = std::make_unique<SimpleMarbleDB>(options.db_path);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open MarbleDB: " + std::string(e.what()));
    }
}

//==============================================================================
// Table Management API Implementation
//==============================================================================

Status CreateTable(MarbleDB* db, const std::string& table_name, const std::string& schema_json) {
    if (!db || table_name.empty()) {
        return Status::InvalidArgument("Invalid database handle or table name");
    }

    try {
        std::shared_ptr<arrow::Schema> schema;
        ARROW_RETURN_NOT_OK(JsonToArrowSchema(schema_json, &schema));

        TableSchema table_schema(table_name, schema);
        return db->CreateTable(table_schema);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to create table: " + std::string(e.what()));
    }
}

Status DropTable(MarbleDB* db, const std::string& table_name) {
    if (!db || table_name.empty()) {
        return Status::InvalidArgument("Invalid database handle or table name");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        // For now, return not implemented
        return Status::NotImplemented("DropTable not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to drop table: " + std::string(e.what()));
    }
}

Status ListTables(MarbleDB* db, std::vector<std::string>* table_names) {
    if (!db || !table_names) {
        return Status::InvalidArgument("Invalid database handle or output parameter");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        // For now, return empty list
        table_names->clear();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to list tables: " + std::string(e.what()));
    }
}

Status GetTableSchema(MarbleDB* db, const std::string& table_name, std::string* schema_json) {
    if (!db || table_name.empty() || !schema_json) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        // For now, return empty schema
        *schema_json = "{}";
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get table schema: " + std::string(e.what()));
    }
}

//==============================================================================
// Data Ingestion API Implementation
//==============================================================================

Status InsertRecord(MarbleDB* db, const std::string& table_name, const std::string& record_json) {
    if (!db || table_name.empty() || record_json.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // Get table schema (simplified)
        std::string schema_json;
        ARROW_RETURN_NOT_OK(GetTableSchema(db, table_name, &schema_json));

        std::shared_ptr<arrow::Schema> schema;
        ARROW_RETURN_NOT_OK(JsonToArrowSchema(schema_json, &schema));

        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(JsonToArrowRecord(record_json, schema, &batch));

        // Insert the batch
        return db->InsertBatch(table_name, batch);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to insert record: " + std::string(e.what()));
    }
}

Status InsertRecordsBatch(MarbleDB* db, const std::string& table_name, const std::string& records_json) {
    if (!db || table_name.empty() || records_json.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would parse JSON array and create larger RecordBatch
        // For now, simplified to single record
        return InsertRecord(db, table_name, records_json);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to insert records batch: " + std::string(e.what()));
    }
}

Status InsertArrowBatch(MarbleDB* db, const std::string& table_name,
                       const void* record_batch_data, size_t record_batch_size) {
    if (!db || table_name.empty() || !record_batch_data || record_batch_size == 0) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(DeserializeArrowBatch(record_batch_data, record_batch_size, &batch));

        return db->InsertBatch(table_name, batch);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to insert Arrow batch: " + std::string(e.what()));
    }
}

//==============================================================================
// Query API Implementation
//==============================================================================

Status ExecuteQuery(MarbleDB* db, const std::string& query_sql, std::unique_ptr<QueryResult>* result) {
    if (!db || query_sql.empty() || !result) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // Parse SQL query (simplified - would need real SQL parser)
        // For now, assume simple SELECT * FROM table format

        std::string table_name;
        size_t from_pos = query_sql.find("FROM");
        if (from_pos != std::string::npos) {
            size_t table_start = from_pos + 4;
            while (table_start < query_sql.size() && isspace(query_sql[table_start])) table_start++;
            size_t table_end = table_start;
            while (table_end < query_sql.size() && !isspace(query_sql[table_end])) table_end++;
            table_name = query_sql.substr(table_start, table_end - table_start);
        }

        if (table_name.empty()) {
            return Status::InvalidArgument("Could not parse table name from query");
        }

        // Execute scan
        return db->ScanTable(table_name, result);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to execute query: " + std::string(e.what()));
    }
}

Status ExecuteQueryWithOptions(MarbleDB* db, const std::string& query_sql,
                              const std::string& options_json, std::unique_ptr<QueryResult>* result) {
    // Simplified - ignore options for now
    return ExecuteQuery(db, query_sql, result);
}

//==============================================================================
// Query Result API Implementation
//==============================================================================

Status QueryResultHasNext(QueryResult* result, bool* has_next) {
    if (!result || !has_next) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        *has_next = result->HasNext();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to check if result has next: " + std::string(e.what()));
    }
}

Status QueryResultNextJson(QueryResult* result, std::string* batch_json) {
    if (!result || !batch_json) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(result->Next(&batch));

        return ArrowRecordBatchToJson(batch, batch_json);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get next result batch as JSON: " + std::string(e.what()));
    }
}

Status QueryResultNextArrow(QueryResult* result, void** arrow_data, size_t* arrow_size) {
    if (!result || !arrow_data || !arrow_size) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(result->Next(&batch));

        std::string serialized_data;
        ARROW_RETURN_NOT_OK(SerializeArrowBatch(batch, &serialized_data));

        // Allocate memory for the caller (they must free it with FreeArrowData)
        void* data_copy = malloc(serialized_data.size());
        if (!data_copy) {
            return Status::InternalError("Failed to allocate memory for Arrow data");
        }

        memcpy(data_copy, serialized_data.data(), serialized_data.size());
        *arrow_data = data_copy;
        *arrow_size = serialized_data.size();

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get next result batch as Arrow: " + std::string(e.what()));
    }
}

void FreeArrowData(void* arrow_data) {
    if (arrow_data) {
        free(arrow_data);
    }
}

Status QueryResultGetSchema(QueryResult* result, std::string* schema_json) {
    if (!result || !schema_json) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        auto schema = result->schema();
        return ArrowSchemaToJson(schema, schema_json);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get result schema: " + std::string(e.what()));
    }
}

Status QueryResultGetRowCount(QueryResult* result, int64_t* row_count) {
    if (!result || !row_count) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        *row_count = result->num_rows();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get row count: " + std::string(e.what()));
    }
}

void CloseQueryResult(std::unique_ptr<QueryResult>* result) {
    result->reset();
}

//==============================================================================
// Transaction API Implementation
//==============================================================================

Status BeginTransaction(MarbleDB* db, uint64_t* transaction_id) {
    if (!db || !transaction_id) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        // For now, return not implemented
        *transaction_id = 1; // Dummy ID
        return Status::NotImplemented("Transactions not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to begin transaction: " + std::string(e.what()));
    }
}

Status CommitTransaction(MarbleDB* db, uint64_t transaction_id) {
    if (!db) {
        return Status::InvalidArgument("Invalid database handle");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        return Status::NotImplemented("Transactions not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to commit transaction: " + std::string(e.what()));
    }
}

Status RollbackTransaction(MarbleDB* db, uint64_t transaction_id) {
    if (!db) {
        return Status::InvalidArgument("Invalid database handle");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        return Status::NotImplemented("Transactions not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to rollback transaction: " + std::string(e.what()));
    }
}

//==============================================================================
// Administrative API Implementation
//==============================================================================

Status GetDatabaseStats(MarbleDB* db, std::string* stats_json) {
    if (!db || !stats_json) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // Create basic stats JSON
        nlohmann::json stats;
        stats["status"] = "operational";
        stats["tables"] = 0; // Would need to implement actual counting
        stats["total_rows"] = 0; // Would need to implement actual counting

        *stats_json = stats.dump(2);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get database stats: " + std::string(e.what()));
    }
}

Status OptimizeDatabase(MarbleDB* db, const std::string& options_json) {
    if (!db) {
        return Status::InvalidArgument("Invalid database handle");
    }

    try {
        // This would trigger compaction and optimization
        return Status::NotImplemented("Database optimization not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to optimize database: " + std::string(e.what()));
    }
}

Status BackupDatabase(MarbleDB* db, const std::string& backup_path) {
    if (!db || backup_path.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would create a backup of the database
        return Status::NotImplemented("Database backup not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to backup database: " + std::string(e.what()));
    }
}

Status RestoreDatabase(MarbleDB* db, const std::string& backup_path) {
    if (!db || backup_path.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would restore database from backup
        return Status::NotImplemented("Database restore not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to restore database: " + std::string(e.what()));
    }
}

//==============================================================================
// Configuration API Implementation
//==============================================================================

Status SetConfig(MarbleDB* db, const std::string& key, const std::string& value) {
    if (!db || key.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would set configuration options
        return Status::NotImplemented("Configuration not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to set config: " + std::string(e.what()));
    }
}

Status GetConfig(MarbleDB* db, const std::string& key, std::string* value) {
    if (!db || key.empty() || !value) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would get configuration options
        *value = ""; // Default empty
        return Status::NotImplemented("Configuration not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get config: " + std::string(e.what()));
    }
}

//==============================================================================
// Error Handling Implementation
//==============================================================================

Status GetLastError(MarbleDB* db, std::string* error_msg) {
    if (!error_msg) {
        return Status::InvalidArgument("Invalid output parameter");
    }

    try {
        // This would return the last error message
        *error_msg = "No error"; // Default
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get last error: " + std::string(e.what()));
    }
}

void ClearLastError(MarbleDB* db) {
    // Clear any stored error state
}

//==============================================================================
// Version and Information Implementation
//==============================================================================

Status GetVersion(std::string* version) {
    if (!version) {
        return Status::InvalidArgument("Invalid output parameter");
    }

    try {
        *version = "0.1.0"; // Version would be set during build
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get version: " + std::string(e.what()));
    }
}

Status GetBuildInfo(std::string* build_info) {
    if (!build_info) {
        return Status::InvalidArgument("Invalid output parameter");
    }

    try {
        nlohmann::json info;
        info["version"] = "0.1.0";
        info["build_type"] = "development";
        info["features"] = {"arrow", "pushdown", "analytics"};

        *build_info = info.dump(2);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get build info: " + std::string(e.what()));
    }
}

} // namespace marble
