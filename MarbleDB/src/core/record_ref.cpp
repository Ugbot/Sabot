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

} // namespace marble

