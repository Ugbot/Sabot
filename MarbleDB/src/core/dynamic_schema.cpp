/************************************************************************
MarbleDB Dynamic Schema Implementation
**************************************************************************/

#include "marble/dynamic_schema.h"
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/record_batch.h>
#include <stdexcept>

namespace marble {

// DynamicField implementation
std::shared_ptr<arrow::Field> DynamicField::ToArrowField() const {
    std::shared_ptr<arrow::DataType> arrow_type;
    
    switch (data_type) {
        case arrow::Type::BOOL:
            arrow_type = arrow::boolean();
            break;
        case arrow::Type::INT8:
            arrow_type = arrow::int8();
            break;
        case arrow::Type::INT16:
            arrow_type = arrow::int16();
            break;
        case arrow::Type::INT32:
            arrow_type = arrow::int32();
            break;
        case arrow::Type::INT64:
            arrow_type = arrow::int64();
            break;
        case arrow::Type::UINT8:
            arrow_type = arrow::uint8();
            break;
        case arrow::Type::UINT16:
            arrow_type = arrow::uint16();
            break;
        case arrow::Type::UINT32:
            arrow_type = arrow::uint32();
            break;
        case arrow::Type::UINT64:
            arrow_type = arrow::uint64();
            break;
        case arrow::Type::FLOAT:
            arrow_type = arrow::float32();
            break;
        case arrow::Type::DOUBLE:
            arrow_type = arrow::float64();
            break;
        case arrow::Type::STRING:
            arrow_type = arrow::utf8();
            break;
        case arrow::Type::BINARY:
            arrow_type = arrow::binary();
            break;
        default:
            arrow_type = arrow::utf8();  // Default to string
            break;
    }
    
    return arrow::field(name, arrow_type, nullable);
}

// DynValue implementation
arrow::Type::type DynValue::arrow_type() const {
    return std::visit([](auto&& arg) -> arrow::Type::type {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) return arrow::Type::NA;
        else if constexpr (std::is_same_v<T, bool>) return arrow::Type::BOOL;
        else if constexpr (std::is_same_v<T, int8_t>) return arrow::Type::INT8;
        else if constexpr (std::is_same_v<T, int16_t>) return arrow::Type::INT16;
        else if constexpr (std::is_same_v<T, int32_t>) return arrow::Type::INT32;
        else if constexpr (std::is_same_v<T, int64_t>) return arrow::Type::INT64;
        else if constexpr (std::is_same_v<T, uint8_t>) return arrow::Type::UINT8;
        else if constexpr (std::is_same_v<T, uint16_t>) return arrow::Type::UINT16;
        else if constexpr (std::is_same_v<T, uint32_t>) return arrow::Type::UINT32;
        else if constexpr (std::is_same_v<T, uint64_t>) return arrow::Type::UINT64;
        else if constexpr (std::is_same_v<T, float>) return arrow::Type::FLOAT;
        else if constexpr (std::is_same_v<T, double>) return arrow::Type::DOUBLE;
        else if constexpr (std::is_same_v<T, std::string>) return arrow::Type::STRING;
        else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) return arrow::Type::BINARY;
        else return arrow::Type::NA;
    }, value_);
}

std::shared_ptr<arrow::Scalar> DynValue::ToArrowScalar() const {
    return std::visit([](auto&& arg) -> std::shared_ptr<arrow::Scalar> {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            return arrow::MakeNullScalar(arrow::null());
        } else if constexpr (std::is_same_v<T, bool>) {
            return arrow::MakeScalar(arg);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return arrow::MakeScalar(arg);
        } else if constexpr (std::is_same_v<T, double>) {
            return arrow::MakeScalar(arg);
        } else if constexpr (std::is_same_v<T, std::string>) {
            return arrow::MakeScalar(arg);
        } else {
            return arrow::MakeNullScalar(arrow::null());
        }
    }, value_);
}

int DynValue::Compare(const DynValue& other) const {
    // Simple comparison - can be enhanced
    if (is_null() && other.is_null()) return 0;
    if (is_null()) return -1;
    if (other.is_null()) return 1;
    
    // Type-specific comparison
    if (auto v1 = as_int64()) {
        if (auto v2 = other.as_int64()) {
            if (*v1 < *v2) return -1;
            if (*v1 > *v2) return 1;
            return 0;
        }
    }
    
    if (auto v1 = as_string()) {
        if (auto v2 = other.as_string()) {
            return v1->compare(*v2);
        }
    }
    
    return 0;
}

// DynSchema implementation
DynSchema::DynSchema(const std::vector<DynamicField>& fields, size_t primary_key_index)
    : fields_(fields), primary_key_index_(primary_key_index) {
    
    if (primary_key_index >= fields.size()) {
        throw std::invalid_argument("Primary key index out of range");
    }
    
    // Build Arrow schema
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    arrow_fields.reserve(fields.size());
    
    for (const auto& field : fields) {
        arrow_fields.push_back(field.ToArrowField());
    }
    
    arrow_schema_ = std::make_shared<arrow::Schema>(arrow_fields);
}

std::shared_ptr<DynSchema> DynSchema::FromArrowSchema(
    const arrow::Schema& schema,
    size_t primary_key_index) {
    
    std::vector<DynamicField> fields;
    fields.reserve(schema.num_fields());
    
    for (const auto& field : schema.fields()) {
        fields.emplace_back(
            field->name(),
            field->type()->id(),
            field->nullable()
        );
    }
    
    return std::make_shared<DynSchema>(fields, primary_key_index);
}

const DynamicField* DynSchema::GetField(const std::string& name) const {
    for (const auto& field : fields_) {
        if (field.name == name) {
            return &field;
        }
    }
    return nullptr;
}

// DynRecord implementation
DynRecord::DynRecord(std::shared_ptr<DynSchema> schema, std::vector<DynValue> values)
    : schema_(schema), values_(std::move(values)) {
    
    if (values_.size() != schema->num_fields()) {
        throw std::invalid_argument("Value count doesn't match schema");
    }
}

Status DynRecord::Validate() const {
    if (values_.size() != schema_->num_fields()) {
        return Status::InvalidArgument("Field count mismatch");
    }
    
    for (size_t i = 0; i < values_.size(); ++i) {
        const auto& field = schema_->fields()[i];
        const auto& value = values_[i];
        
        // Check primary key is not null
        if (i == schema_->primary_key_index() && value.is_null()) {
            return Status::InvalidArgument("Primary key cannot be null");
        }
        
        // Check nullability
        if (!field.nullable && value.is_null()) {
            return Status::InvalidArgument("Non-nullable field is null: " + field.name);
        }
    }
    
    return Status::OK();
}

const DynValue& DynRecord::GetField(size_t index) const {
    if (index >= values_.size()) {
        throw std::out_of_range("Field index out of range");
    }
    return values_[index];
}

const DynValue& DynRecord::GetField(const std::string& name) const {
    const auto* field = schema_->GetField(name);
    if (!field) {
        throw std::invalid_argument("Field not found: " + name);
    }
    
    // Find index
    for (size_t i = 0; i < schema_->fields().size(); ++i) {
        if (schema_->fields()[i].name == name) {
            return values_[i];
        }
    }
    
    throw std::invalid_argument("Field not found: " + name);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DynRecord::ToArrowBatch() const {
    // Build single-row RecordBatch
    DynRecordBatchBuilder builder(schema_);
    ARROW_RETURN_NOT_OK(builder.Append(*this));
    return builder.Finish();
}

arrow::Result<std::shared_ptr<DynRecord>> DynRecord::FromArrowBatch(
    std::shared_ptr<DynSchema> schema,
    const arrow::RecordBatch& batch,
    size_t row_index) {
    
    if (row_index >= static_cast<size_t>(batch.num_rows())) {
        return arrow::Status::Invalid("Row index out of range");
    }
    
    std::vector<DynValue> values;
    values.reserve(schema->num_fields());
    
    for (size_t i = 0; i < schema->num_fields(); ++i) {
        auto column = batch.column(i);
        
        if (column->IsNull(row_index)) {
            values.emplace_back();  // Null value
            continue;
        }
        
        // Extract value based on type
        switch (column->type()->id()) {
            case arrow::Type::INT64: {
                auto array = std::static_pointer_cast<arrow::Int64Array>(column);
                values.emplace_back(array->Value(row_index));
                break;
            }
            case arrow::Type::DOUBLE: {
                auto array = std::static_pointer_cast<arrow::DoubleArray>(column);
                values.emplace_back(array->Value(row_index));
                break;
            }
            case arrow::Type::STRING: {
                auto array = std::static_pointer_cast<arrow::StringArray>(column);
                values.emplace_back(array->GetString(row_index));
                break;
            }
            case arrow::Type::BOOL: {
                auto array = std::static_pointer_cast<arrow::BooleanArray>(column);
                values.emplace_back(array->Value(row_index));
                break;
            }
            default:
                values.emplace_back();  // Unsupported type -> null
                break;
        }
    }
    
    return std::make_shared<DynRecord>(schema, std::move(values));
}

// DynRecordBatchBuilder implementation
DynRecordBatchBuilder::DynRecordBatchBuilder(std::shared_ptr<DynSchema> schema)
    : schema_(schema), num_records_(0) {
    
    // Create array builders for each field
    builders_.reserve(schema->num_fields());
    
    for (const auto& field : schema->fields()) {
        switch (field.data_type) {
            case arrow::Type::INT64:
                builders_.push_back(std::make_unique<arrow::Int64Builder>());
                break;
            case arrow::Type::DOUBLE:
                builders_.push_back(std::make_unique<arrow::DoubleBuilder>());
                break;
            case arrow::Type::STRING:
                builders_.push_back(std::make_unique<arrow::StringBuilder>());
                break;
            case arrow::Type::BOOL:
                builders_.push_back(std::make_unique<arrow::BooleanBuilder>());
                break;
            default:
                builders_.push_back(std::make_unique<arrow::StringBuilder>());
                break;
        }
    }
}

Status DynRecordBatchBuilder::Append(const DynRecord& record) {
    if (record.schema().get() != schema_.get()) {
        return Status::InvalidArgument("Record schema doesn't match builder schema");
    }
    
    for (size_t i = 0; i < schema_->num_fields(); ++i) {
        const auto& value = record.GetField(i);
        auto& builder = builders_[i];
        
        if (value.is_null()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
            continue;
        }
        
        // Type-specific append
        const auto& field = schema_->fields()[i];
        switch (field.data_type) {
            case arrow::Type::INT64: {
                auto typed_builder = static_cast<arrow::Int64Builder*>(builder.get());
                auto val = value.as_int64();
                if (val) {
                    ARROW_RETURN_NOT_OK(typed_builder->Append(*val));
                } else {
                    ARROW_RETURN_NOT_OK(typed_builder->AppendNull());
                }
                break;
            }
            case arrow::Type::DOUBLE: {
                auto typed_builder = static_cast<arrow::DoubleBuilder*>(builder.get());
                auto val = value.as_double();
                if (val) {
                    ARROW_RETURN_NOT_OK(typed_builder->Append(*val));
                } else {
                    ARROW_RETURN_NOT_OK(typed_builder->AppendNull());
                }
                break;
            }
            case arrow::Type::STRING: {
                auto typed_builder = static_cast<arrow::StringBuilder*>(builder.get());
                auto val = value.as_string();
                if (val) {
                    ARROW_RETURN_NOT_OK(typed_builder->Append(*val));
                } else {
                    ARROW_RETURN_NOT_OK(typed_builder->AppendNull());
                }
                break;
            }
            case arrow::Type::BOOL: {
                auto typed_builder = static_cast<arrow::BooleanBuilder*>(builder.get());
                auto val = value.as_bool();
                if (val) {
                    ARROW_RETURN_NOT_OK(typed_builder->Append(*val));
                } else {
                    ARROW_RETURN_NOT_OK(typed_builder->AppendNull());
                }
                break;
            }
            default:
                ARROW_RETURN_NOT_OK(builder->AppendNull());
                break;
        }
    }
    
    num_records_++;
    return Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DynRecordBatchBuilder::Finish() {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(builders_.size());
    
    for (auto& builder : builders_) {
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder->Finish(&array));
        arrays.push_back(array);
    }
    
    return arrow::RecordBatch::Make(schema_->arrow_schema(), num_records_, arrays);
}

void DynRecordBatchBuilder::Reset() {
    for (auto& builder : builders_) {
        builder->Reset();
    }
    num_records_ = 0;
}

} // namespace marble

