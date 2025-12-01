/************************************************************************
MarbleDB Zero-Copy RecordRef Implementation
**************************************************************************/

#include "marble/record_ref.h"
#include "marble/record.h"
#include <arrow/array.h>
#include <stdexcept>

namespace marble {

// FieldRef implementation
FieldRef FieldRef::FromArray(const arrow::Array& array, size_t offset) {
    FieldRef ref;
    
    if (offset >= array.length() || array.IsNull(offset)) {
        ref.type_ = Type::kNull;
        return ref;
    }
    
    switch (array.type()->id()) {
        case arrow::Type::INT64: {
            auto typed_array = static_cast<const arrow::Int64Array*>(&array);
            ref.type_ = Type::kInt64;
            ref.int64_val = typed_array->Value(offset);
            break;
        }
        case arrow::Type::DOUBLE: {
            auto typed_array = static_cast<const arrow::DoubleArray*>(&array);
            ref.type_ = Type::kDouble;
            ref.double_val = typed_array->Value(offset);
            break;
        }
        case arrow::Type::STRING: {
            auto typed_array = static_cast<const arrow::StringArray*>(&array);
            ref.type_ = Type::kString;
            auto view = typed_array->GetView(offset);
            ref.string_val.data = view.data();
            ref.string_val.length = view.size();
            break;
        }
        case arrow::Type::BINARY: {
            auto typed_array = static_cast<const arrow::BinaryArray*>(&array);
            ref.type_ = Type::kBinary;
            auto view = typed_array->GetView(offset);
            ref.string_val.data = view.data();
            ref.string_val.length = view.size();
            break;
        }
        case arrow::Type::BOOL: {
            auto typed_array = static_cast<const arrow::BooleanArray*>(&array);
            ref.type_ = Type::kBoolean;
            ref.bool_val = typed_array->Value(offset);
            break;
        }
        default:
            ref.type_ = Type::kNull;
            break;
    }
    
    return ref;
}

std::optional<int64_t> FieldRef::as_int64() const {
    if (type_ != Type::kInt64) return std::nullopt;
    return int64_val;
}

std::optional<double> FieldRef::as_double() const {
    if (type_ != Type::kDouble) return std::nullopt;
    return double_val;
}

std::optional<std::string_view> FieldRef::as_string() const {
    if (type_ != Type::kString) return std::nullopt;
    return std::string_view(string_val.data, string_val.length);
}

std::optional<std::string_view> FieldRef::as_binary() const {
    if (type_ != Type::kBinary) return std::nullopt;
    return std::string_view(string_val.data, string_val.length);
}

std::optional<bool> FieldRef::as_boolean() const {
    if (type_ != Type::kBoolean) return std::nullopt;
    return bool_val;
}

// ArrowRecordRef implementation
ArrowRecordRef::ArrowRecordRef(const std::shared_ptr<arrow::RecordBatch>& batch,
                               size_t offset,
                               const std::shared_ptr<arrow::Schema>& full_schema)
    : batch_(batch), offset_(offset), full_schema_(full_schema) {
    
    if (!batch) {
        throw std::invalid_argument("RecordBatch cannot be null");
    }
    if (offset >= static_cast<size_t>(batch->num_rows())) {
        throw std::out_of_range("Offset exceeds batch size");
    }
}

int ArrowRecordRef::field_index(const std::string& name) const {
    // Check cache first
    auto it = field_index_cache_.find(name);
    if (it != field_index_cache_.end()) {
        return it->second;
    }
    
    // Lookup in schema
    auto result = full_schema_->GetFieldIndex(name);
    if (result == -1) {
        return -1;
    }
    
    // Cache for future lookups
    field_index_cache_[name] = result;
    return result;
}

FieldRef ArrowRecordRef::get(const std::string& field_name) const {
    int idx = field_index(field_name);
    if (idx < 0) {
        return FieldRef();  // Null field
    }
    
    auto column = batch_->column(idx);
    return FieldRef::FromArray(*column, offset_);
}

std::optional<int64_t> ArrowRecordRef::get_int64(const std::string& field) const {
    return get(field).as_int64();
}

std::optional<double> ArrowRecordRef::get_double(const std::string& field) const {
    return get(field).as_double();
}

std::optional<std::string_view> ArrowRecordRef::get_string(const std::string& field) const {
    return get(field).as_string();
}

std::optional<std::string_view> ArrowRecordRef::get_binary(const std::string& field) const {
    return get(field).as_binary();
}

std::optional<bool> ArrowRecordRef::get_boolean(const std::string& field) const {
    return get(field).as_boolean();
}

std::shared_ptr<Key> ArrowRecordRef::key() const {
    // Assume first user column (after _null and _ts) is the primary key
    // TODO: Make this configurable via schema metadata
    if (batch_->num_columns() < 3) {
        return nullptr;
    }
    
    auto key_column = batch_->column(2);
    
    // Create appropriate key type based on column type
    if (key_column->type()->id() == arrow::Type::INT64) {
        auto array = std::static_pointer_cast<arrow::Int64Array>(key_column);
        int64_t key_val = array->Value(offset_);
        // Return Int64Key for int64 values
        return std::make_shared<Int64Key>(key_val);
    } else if (key_column->type()->id() == arrow::Type::STRING) {
        auto array = std::static_pointer_cast<arrow::StringArray>(key_column);
        std::string key_val = array->GetString(offset_);
        // TODO: Return StringKey
        return nullptr;
    }
    
    return nullptr;
}

bool ArrowRecordRef::is_tombstone() const {
    // Column 0 is _null flag
    if (batch_->num_columns() < 1) {
        return false;
    }
    
    auto null_column = batch_->column(0);
    if (null_column->type()->id() != arrow::Type::BOOL) {
        return false;
    }
    
    auto null_array = std::static_pointer_cast<arrow::BooleanArray>(null_column);
    return null_array->Value(offset_);
}

uint32_t ArrowRecordRef::timestamp() const {
    // Column 1 is _ts timestamp
    if (batch_->num_columns() < 2) {
        return 0;
    }
    
    auto ts_column = batch_->column(1);
    if (ts_column->type()->id() != arrow::Type::UINT32) {
        return 0;
    }
    
    auto ts_array = std::static_pointer_cast<arrow::UInt32Array>(ts_column);
    return ts_array->Value(offset_);
}

std::vector<std::string> ArrowRecordRef::field_names() const {
    std::vector<std::string> names;
    names.reserve(full_schema_->num_fields());
    
    for (const auto& field : full_schema_->fields()) {
        names.push_back(field->name());
    }
    
    return names;
}

//==============================================================================
// SimpleRecord Implementation
//==============================================================================

SimpleRecord::SimpleRecord(std::shared_ptr<Key> key, std::shared_ptr<arrow::RecordBatch> batch, int64_t row_index)
    : key_(std::move(key)), batch_(std::move(batch)), row_index_(row_index),
      begin_ts_(0), commit_ts_(0) {}

std::shared_ptr<Key> SimpleRecord::GetKey() const {
    return key_;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> SimpleRecord::ToRecordBatch() const {
    // Extract single row as RecordBatch
    return batch_->Slice(row_index_, 1);
}

std::shared_ptr<arrow::Schema> SimpleRecord::GetArrowSchema() const {
    return batch_->schema();
}

std::unique_ptr<RecordRef> SimpleRecord::AsRecordRef() const {
    // Inline implementation of RecordRef for SimpleRecord
    // This wraps the SimpleRecord's key, batch, and row_index
    class SimpleRecordRef : public RecordRef {
    private:
        std::shared_ptr<Key> key_;
        std::shared_ptr<arrow::RecordBatch> batch_;
        int64_t row_index_;

    public:
        SimpleRecordRef(std::shared_ptr<Key> key,
                       std::shared_ptr<arrow::RecordBatch> batch,
                       int64_t row_index)
            : key_(std::move(key)), batch_(std::move(batch)), row_index_(row_index) {}

        std::shared_ptr<Key> key() const override {
            return key_;
        }

        arrow::Result<std::shared_ptr<arrow::Scalar>> GetField(const std::string& field_name) const override {
            auto schema = batch_->schema();
            int idx = schema->GetFieldIndex(field_name);
            if (idx < 0) {
                return arrow::Status::KeyError("Field not found: " + field_name);
            }
            return batch_->column(idx)->GetScalar(row_index_);
        }

        arrow::Result<std::vector<std::shared_ptr<arrow::Scalar>>> GetFields() const override {
            std::vector<std::shared_ptr<arrow::Scalar>> scalars;
            scalars.reserve(batch_->num_columns());
            for (int i = 0; i < batch_->num_columns(); ++i) {
                auto scalar_result = batch_->column(i)->GetScalar(row_index_);
                if (!scalar_result.ok()) {
                    return scalar_result.status();
                }
                scalars.push_back(*scalar_result);
            }
            return scalars;
        }

        size_t Size() const override {
            // Estimate total size as sum of column data for this row
            size_t total = 0;
            for (int i = 0; i < batch_->num_columns(); ++i) {
                // Rough estimate: 64 bytes per field (covers most types)
                total += 64;
            }
            return total;
        }
    };

    return std::make_unique<SimpleRecordRef>(key_, batch_, row_index_);
}

// MVCC versioning support
void SimpleRecord::SetMVCCInfo(uint64_t begin_ts, uint64_t commit_ts) {
    begin_ts_ = begin_ts;
    commit_ts_ = commit_ts;
}

uint64_t SimpleRecord::GetBeginTimestamp() const {
    return begin_ts_;
}

uint64_t SimpleRecord::GetCommitTimestamp() const {
    return commit_ts_;
}

bool SimpleRecord::IsVisible(uint64_t snapshot_ts) const {
    // Uncommitted records (commit_ts=0) are never visible to other transactions
    if (commit_ts_ == 0) {
        return false;
    }
    // Committed records are visible if committed before or at snapshot time
    return commit_ts_ <= snapshot_ts;
}

} // namespace marble

