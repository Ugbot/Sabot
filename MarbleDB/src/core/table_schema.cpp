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

#include "marble/table_schema.h"
#include <arrow/builder.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <sstream>
#include <algorithm>
#include <cstring>
#include <set>

namespace marble {

// ============================================================================
// CompositeKeyDef Implementation
// ============================================================================

std::string CompositeKeyDef::ToJson() const {
    std::ostringstream ss;
    ss << "{\"partition\":[";
    for (size_t i = 0; i < partition_columns.size(); ++i) {
        if (i > 0) ss << ",";
        ss << "\"" << partition_columns[i] << "\"";
    }
    ss << "],\"clustering\":[";
    for (size_t i = 0; i < clustering_columns.size(); ++i) {
        if (i > 0) ss << ",";
        ss << "\"" << clustering_columns[i] << "\"";
    }
    ss << "]}";
    return ss.str();
}

Status CompositeKeyDef::FromJson(const std::string& json, CompositeKeyDef* def) {
    // Simple JSON parsing for the key definition
    // Format: {"partition":["col1","col2"],"clustering":["col3"]}
    def->partition_columns.clear();
    def->clustering_columns.clear();

    size_t partition_start = json.find("\"partition\":[");
    if (partition_start != std::string::npos) {
        size_t arr_start = json.find('[', partition_start);
        size_t arr_end = json.find(']', arr_start);
        if (arr_start != std::string::npos && arr_end != std::string::npos) {
            std::string arr = json.substr(arr_start + 1, arr_end - arr_start - 1);
            size_t pos = 0;
            while ((pos = arr.find('"', pos)) != std::string::npos) {
                size_t end = arr.find('"', pos + 1);
                if (end != std::string::npos) {
                    def->partition_columns.push_back(arr.substr(pos + 1, end - pos - 1));
                    pos = end + 1;
                } else {
                    break;
                }
            }
        }
    }

    size_t clustering_start = json.find("\"clustering\":[");
    if (clustering_start != std::string::npos) {
        size_t arr_start = json.find('[', clustering_start);
        size_t arr_end = json.find(']', arr_start);
        if (arr_start != std::string::npos && arr_end != std::string::npos) {
            std::string arr = json.substr(arr_start + 1, arr_end - arr_start - 1);
            size_t pos = 0;
            while ((pos = arr.find('"', pos)) != std::string::npos) {
                size_t end = arr.find('"', pos + 1);
                if (end != std::string::npos) {
                    def->clustering_columns.push_back(arr.substr(pos + 1, end - pos - 1));
                    pos = end + 1;
                } else {
                    break;
                }
            }
        }
    }

    return Status::OK();
}

// ColumnStatistics is implemented in sstable_arrow.cpp

// ============================================================================
// WideTableSchema Implementation
// ============================================================================

WideTableSchema::WideTableSchema(std::shared_ptr<arrow::Schema> arrow_schema,
                         CompositeKeyDef key_def)
    : arrow_schema_(std::move(arrow_schema)),
      key_def_(std::move(key_def)) {
    BuildColumnIndices();
}

WideTableSchema::WideTableSchema(std::shared_ptr<arrow::Schema> arrow_schema,
                         const std::string& primary_key_column)
    : arrow_schema_(std::move(arrow_schema)) {
    // Convert single primary key to composite key format
    key_def_.partition_columns = {primary_key_column};
    key_def_.clustering_columns = {};
    BuildColumnIndices();
}

void WideTableSchema::BuildColumnIndices() {
    partition_indices_.clear();
    clustering_indices_.clear();
    value_indices_.clear();

    // Build partition column indices
    for (const auto& col_name : key_def_.partition_columns) {
        int idx = arrow_schema_->GetFieldIndex(col_name);
        if (idx >= 0) {
            partition_indices_.push_back(idx);
        }
    }

    // Build clustering column indices
    for (const auto& col_name : key_def_.clustering_columns) {
        int idx = arrow_schema_->GetFieldIndex(col_name);
        if (idx >= 0) {
            clustering_indices_.push_back(idx);
        }
    }

    // Build value column indices (all columns not in key)
    std::set<int> key_indices;
    for (int idx : partition_indices_) key_indices.insert(idx);
    for (int idx : clustering_indices_) key_indices.insert(idx);

    for (int i = 0; i < arrow_schema_->num_fields(); ++i) {
        if (key_indices.find(i) == key_indices.end()) {
            value_indices_.push_back(i);
        }
    }
}

uint32_t WideTableSchema::HashCombine(uint32_t seed, uint32_t value) {
    // FNV-1a style hash combine
    return seed ^ (value + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

uint32_t WideTableSchema::HashPartitionColumns(const arrow::RecordBatch& batch, int64_t row_idx) const {
    uint32_t hash = 0;

    for (int idx : partition_indices_) {
        auto array = batch.column(idx);
        auto scalar_result = array->GetScalar(row_idx);
        if (!scalar_result.ok()) continue;

        auto scalar = scalar_result.ValueOrDie();
        if (!scalar->is_valid) {
            hash = HashCombine(hash, 0);
            continue;
        }

        // Hash based on type
        uint32_t value_hash = 0;
        if (auto int_val = std::dynamic_pointer_cast<arrow::Int64Scalar>(scalar)) {
            value_hash = static_cast<uint32_t>(int_val->value);
        } else if (auto uint_val = std::dynamic_pointer_cast<arrow::UInt64Scalar>(scalar)) {
            value_hash = static_cast<uint32_t>(uint_val->value);
        } else if (auto int32_val = std::dynamic_pointer_cast<arrow::Int32Scalar>(scalar)) {
            value_hash = static_cast<uint32_t>(int32_val->value);
        } else if (auto uint32_val = std::dynamic_pointer_cast<arrow::UInt32Scalar>(scalar)) {
            value_hash = uint32_val->value;
        } else if (auto str_val = std::dynamic_pointer_cast<arrow::StringScalar>(scalar)) {
            // FNV-1a hash for strings
            std::string_view sv = str_val->view();
            value_hash = 2166136261u;
            for (char c : sv) {
                value_hash ^= static_cast<uint32_t>(c);
                value_hash *= 16777619u;
            }
        } else if (auto bin_val = std::dynamic_pointer_cast<arrow::BinaryScalar>(scalar)) {
            std::string_view sv = bin_val->view();
            value_hash = 2166136261u;
            for (char c : sv) {
                value_hash ^= static_cast<uint32_t>(c);
                value_hash *= 16777619u;
            }
        } else if (auto dbl_val = std::dynamic_pointer_cast<arrow::DoubleScalar>(scalar)) {
            // Hash the bytes of the double
            uint64_t bits;
            std::memcpy(&bits, &dbl_val->value, sizeof(bits));
            value_hash = static_cast<uint32_t>(bits ^ (bits >> 32));
        }

        hash = HashCombine(hash, value_hash);
    }

    return hash;
}

uint32_t WideTableSchema::EncodeScalarForSort(const std::shared_ptr<arrow::Scalar>& scalar) {
    if (!scalar || !scalar->is_valid) {
        return 0;  // Nulls sort first
    }

    // Encode to preserve sort order
    // For integers: flip sign bit for signed, use directly for unsigned
    // For strings: use first 4 bytes as prefix

    if (auto int64_val = std::dynamic_pointer_cast<arrow::Int64Scalar>(scalar)) {
        // Flip sign bit to make signed comparable as unsigned
        int64_t v = int64_val->value;
        uint64_t u = static_cast<uint64_t>(v) ^ (1ULL << 63);
        return static_cast<uint32_t>(u >> 32);  // Upper 32 bits
    } else if (auto uint64_val = std::dynamic_pointer_cast<arrow::UInt64Scalar>(scalar)) {
        return static_cast<uint32_t>(uint64_val->value >> 32);
    } else if (auto int32_val = std::dynamic_pointer_cast<arrow::Int32Scalar>(scalar)) {
        int32_t v = int32_val->value;
        return static_cast<uint32_t>(v) ^ (1u << 31);
    } else if (auto uint32_val = std::dynamic_pointer_cast<arrow::UInt32Scalar>(scalar)) {
        return uint32_val->value;
    } else if (auto ts_val = std::dynamic_pointer_cast<arrow::TimestampScalar>(scalar)) {
        // Timestamps are int64 microseconds/nanoseconds
        uint64_t u = static_cast<uint64_t>(ts_val->value) ^ (1ULL << 63);
        return static_cast<uint32_t>(u >> 32);
    } else if (auto str_val = std::dynamic_pointer_cast<arrow::StringScalar>(scalar)) {
        // Use first 4 bytes as sort prefix
        std::string_view sv = str_val->view();
        uint32_t result = 0;
        size_t len = std::min(sv.size(), size_t(4));
        for (size_t i = 0; i < len; ++i) {
            result = (result << 8) | static_cast<uint8_t>(sv[i]);
        }
        // Pad with zeros for short strings
        result <<= (8 * (4 - len));
        return result;
    }

    return 0;
}

uint32_t WideTableSchema::EncodeClusteringColumns(const arrow::RecordBatch& batch, int64_t row_idx) const {
    if (clustering_indices_.empty()) {
        return 0;
    }

    uint32_t encoded = 0;
    size_t bits_remaining = 32;
    size_t bits_per_col = 32 / std::max(size_t(1), clustering_indices_.size());

    for (int idx : clustering_indices_) {
        auto array = batch.column(idx);
        auto scalar_result = array->GetScalar(row_idx);
        if (!scalar_result.ok()) continue;

        auto scalar = scalar_result.ValueOrDie();
        uint32_t col_encoded = EncodeScalarForSort(scalar);

        // Take upper bits_per_col bits
        size_t bits = std::min(bits_per_col, bits_remaining);
        uint32_t mask = (1u << bits) - 1;
        encoded = (encoded << bits) | ((col_encoded >> (32 - bits)) & mask);
        bits_remaining -= bits;
    }

    return encoded;
}

uint64_t WideTableSchema::EncodeKey(const arrow::RecordBatch& batch, int64_t row_idx) const {
    uint32_t partition_hash = HashPartitionColumns(batch, row_idx);
    uint32_t clustering_key = EncodeClusteringColumns(batch, row_idx);

    return (static_cast<uint64_t>(partition_hash) << 32) | clustering_key;
}

std::shared_ptr<arrow::UInt64Array> WideTableSchema::EncodeKeys(const arrow::RecordBatch& batch) const {
    arrow::UInt64Builder builder;
    auto status = builder.Reserve(batch.num_rows());
    if (!status.ok()) {
        return nullptr;
    }

    for (int64_t i = 0; i < batch.num_rows(); ++i) {
        uint64_t key = EncodeKey(batch, i);
        status = builder.Append(key);
        if (!status.ok()) {
            return nullptr;
        }
    }

    std::shared_ptr<arrow::UInt64Array> result;
    status = builder.Finish(&result);
    if (!status.ok()) {
        return nullptr;
    }

    return result;
}

Status WideTableSchema::DecodeClusteringKey(uint64_t key,
                                        std::vector<std::shared_ptr<arrow::Scalar>>* clustering_values) const {
    // Note: Full reconstruction requires the original column values
    // This only extracts the encoded prefix for range queries
    clustering_values->clear();

    uint32_t clustering_key = static_cast<uint32_t>(key & 0xFFFFFFFF);

    // For now, return a single uint32 scalar with the encoded value
    // Full decoding would require storing a mapping table
    clustering_values->push_back(arrow::MakeScalar(clustering_key));

    return Status::OK();
}

Status WideTableSchema::GetColumnIndices(const std::vector<std::string>& column_names,
                                     std::vector<int>* indices) const {
    indices->clear();
    indices->reserve(column_names.size());

    for (const auto& name : column_names) {
        int idx = arrow_schema_->GetFieldIndex(name);
        if (idx < 0) {
            return Status::NotFound("Column not found: " + name);
        }
        indices->push_back(idx);
    }

    return Status::OK();
}

Status WideTableSchema::ValidateBatch(const arrow::RecordBatch& batch) const {
    // Check column count
    if (batch.num_columns() != arrow_schema_->num_fields()) {
        return Status::InvalidArgument(
            "Column count mismatch: expected " +
            std::to_string(arrow_schema_->num_fields()) +
            ", got " + std::to_string(batch.num_columns()));
    }

    // Check each column name and type
    for (int i = 0; i < batch.num_columns(); ++i) {
        auto expected_field = arrow_schema_->field(i);
        auto actual_field = batch.schema()->field(i);

        // Check name
        if (expected_field->name() != actual_field->name()) {
            return Status::InvalidArgument(
                "Column name mismatch at index " + std::to_string(i) +
                ": expected '" + expected_field->name() +
                "', got '" + actual_field->name() + "'");
        }

        // Check type (allow compatible types)
        if (!expected_field->type()->Equals(actual_field->type())) {
            // Check for compatible numeric types
            bool compatible = false;
            auto exp_id = expected_field->type()->id();
            auto act_id = actual_field->type()->id();

            // Allow int32 -> int64, etc.
            if ((exp_id == arrow::Type::INT64 && act_id == arrow::Type::INT32) ||
                (exp_id == arrow::Type::INT64 && act_id == arrow::Type::INT64) ||
                (exp_id == arrow::Type::UINT64 && act_id == arrow::Type::UINT32) ||
                (exp_id == arrow::Type::DOUBLE && act_id == arrow::Type::FLOAT)) {
                compatible = true;
            }

            if (!compatible) {
                return Status::InvalidArgument(
                    "Column type mismatch for '" + expected_field->name() +
                    "': expected " + expected_field->type()->ToString() +
                    ", got " + actual_field->type()->ToString());
            }
        }
    }

    // Verify key columns are present
    for (int idx : partition_indices_) {
        if (idx >= batch.num_columns()) {
            return Status::InvalidArgument(
                "Partition key column index " + std::to_string(idx) +
                " out of range");
        }
    }

    for (int idx : clustering_indices_) {
        if (idx >= batch.num_columns()) {
            return Status::InvalidArgument(
                "Clustering key column index " + std::to_string(idx) +
                " out of range");
        }
    }

    return Status::OK();
}

bool WideTableSchema::HasColumn(const std::string& name) const {
    return arrow_schema_->GetFieldIndex(name) >= 0;
}

std::shared_ptr<arrow::DataType> WideTableSchema::GetColumnType(const std::string& name) const {
    int idx = arrow_schema_->GetFieldIndex(name);
    if (idx < 0) {
        return nullptr;
    }
    return arrow_schema_->field(idx)->type();
}

std::string WideTableSchema::ToJson() const {
    std::ostringstream ss;
    ss << "{";

    // Serialize Arrow schema
    auto schema_result = arrow::ipc::SerializeSchema(*arrow_schema_);
    if (schema_result.ok()) {
        auto buffer = schema_result.ValueOrDie();
        ss << "\"arrow_schema_base64\":\"";
        // Base64 encode the schema buffer
        static const char* base64_chars =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        const uint8_t* data = buffer->data();
        int64_t len = buffer->size();
        for (int64_t i = 0; i < len; i += 3) {
            uint32_t n = (static_cast<uint32_t>(data[i]) << 16);
            if (i + 1 < len) n |= (static_cast<uint32_t>(data[i + 1]) << 8);
            if (i + 2 < len) n |= static_cast<uint32_t>(data[i + 2]);

            ss << base64_chars[(n >> 18) & 0x3F];
            ss << base64_chars[(n >> 12) & 0x3F];
            ss << ((i + 1 < len) ? base64_chars[(n >> 6) & 0x3F] : '=');
            ss << ((i + 2 < len) ? base64_chars[n & 0x3F] : '=');
        }
        ss << "\"";
    }

    // Serialize key definition
    ss << ",\"key_def\":" << key_def_.ToJson();

    ss << "}";
    return ss.str();
}

Status WideTableSchema::FromJson(const std::string& json,
                             std::shared_ptr<WideTableSchema>* schema) {
    // Parse key_def
    CompositeKeyDef key_def;
    size_t key_def_start = json.find("\"key_def\":");
    if (key_def_start != std::string::npos) {
        size_t obj_start = json.find('{', key_def_start);
        if (obj_start != std::string::npos) {
            int depth = 1;
            size_t obj_end = obj_start + 1;
            while (depth > 0 && obj_end < json.size()) {
                if (json[obj_end] == '{') depth++;
                else if (json[obj_end] == '}') depth--;
                obj_end++;
            }
            std::string key_def_json = json.substr(obj_start, obj_end - obj_start);
            RETURN_NOT_OK(CompositeKeyDef::FromJson(key_def_json, &key_def));
        }
    }

    // Parse arrow_schema_base64
    size_t schema_start = json.find("\"arrow_schema_base64\":\"");
    if (schema_start == std::string::npos) {
        return Status::InvalidArgument("Missing arrow_schema_base64 in JSON");
    }
    size_t b64_start = json.find('"', schema_start + 22) + 1;
    size_t b64_end = json.find('"', b64_start);
    std::string base64_data = json.substr(b64_start, b64_end - b64_start);

    // Decode base64
    static const int decode_table[] = {
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
        52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
        15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
        -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1
    };

    std::vector<uint8_t> decoded;
    decoded.reserve(base64_data.size() * 3 / 4);

    for (size_t i = 0; i < base64_data.size(); i += 4) {
        uint32_t n = 0;
        int valid_chars = 0;
        for (int j = 0; j < 4 && i + j < base64_data.size(); ++j) {
            char c = base64_data[i + j];
            if (c == '=') break;
            int val = (c < 128) ? decode_table[static_cast<int>(c)] : -1;
            if (val >= 0) {
                n = (n << 6) | val;
                valid_chars++;
            }
        }

        if (valid_chars >= 2) decoded.push_back((n >> 16) & 0xFF);
        if (valid_chars >= 3) decoded.push_back((n >> 8) & 0xFF);
        if (valid_chars >= 4) decoded.push_back(n & 0xFF);
    }

    // Deserialize Arrow schema using BufferReader
    auto buffer = std::make_shared<arrow::Buffer>(decoded.data(), decoded.size());
    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    arrow::ipc::DictionaryMemo memo;
    auto schema_result = arrow::ipc::ReadSchema(buffer_reader.get(), &memo);
    if (!schema_result.ok()) {
        return Status::InvalidArgument("Failed to deserialize Arrow schema: " +
                                       schema_result.status().ToString());
    }

    *schema = std::make_shared<WideTableSchema>(schema_result.ValueOrDie(), key_def);
    return Status::OK();
}

} // namespace marble
