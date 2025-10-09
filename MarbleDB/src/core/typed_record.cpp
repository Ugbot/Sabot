#include "marble/record.h"
#include <arrow/builder.h>
#include <arrow/scalar.h>

namespace marble {

// Global schema registry instance
SchemaRegistry global_schema_registry;

//==============================================================================
// Implementation of template specializations and helpers
//==============================================================================

/**
 * @brief Helper function to get scalar value from Arrow array
 */
template<typename T>
T get_scalar_value(const std::shared_ptr<arrow::Array>& array, size_t index) {
    // This would need proper implementation for each type
    // For now, return default values
    return T{};
}

template<>
int64_t get_scalar_value<int64_t>(const std::shared_ptr<arrow::Array>& array, size_t index) {
    auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
    return int_array->Value(index);
}

template<>
double get_scalar_value<double>(const std::shared_ptr<arrow::Array>& array, size_t index) {
    auto double_array = std::static_pointer_cast<arrow::DoubleArray>(array);
    return double_array->Value(index);
}

template<>
std::string get_scalar_value<std::string>(const std::shared_ptr<arrow::Array>& array, size_t index) {
    auto str_array = std::static_pointer_cast<arrow::StringArray>(array);
    return str_array->GetString(index);
}

/**
 * @brief Helper function to create Arrow array from values
 */
template<typename T>
std::shared_ptr<arrow::Array> create_arrow_array(const std::vector<T>& values) {
    // This would need proper implementation for each type
    // For now, return empty array
    return nullptr;
}

template<>
std::shared_ptr<arrow::Array> create_arrow_array<int64_t>(const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
    auto status = builder.AppendValues(values);
    if (!status.ok()) return nullptr;
    
    std::shared_ptr<arrow::Array> array;
    status = builder.Finish(&array);
    if (!status.ok()) return nullptr;
    
    return array;
}

template<>
std::shared_ptr<arrow::Array> create_arrow_array<double>(const std::vector<double>& values) {
    arrow::DoubleBuilder builder;
    auto status = builder.AppendValues(values);
    if (!status.ok()) return nullptr;
    
    std::shared_ptr<arrow::Array> array;
    status = builder.Finish(&array);
    if (!status.ok()) return nullptr;
    
    return array;
}

template<>
std::shared_ptr<arrow::Array> create_arrow_array<std::string>(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    auto status = builder.AppendValues(values);
    if (!status.ok()) return nullptr;
    
    std::shared_ptr<arrow::Array> array;
    status = builder.Finish(&array);
    if (!status.ok()) return nullptr;
    
    return array;
}

//==============================================================================
// Example: Pure template-based record implementation (no macros)
//==============================================================================

// Define field name constants (compile-time string literals)
constexpr const char id_name[] = "id";
constexpr const char email_name[] = "email";
constexpr const char age_name[] = "age";
constexpr const char balance_name[] = "balance";

// Define field list using templates only
using UserFields = FieldList<
    Field<std::string, id_name, FieldAttribute::kPrimaryKey>,
    Field<std::string, email_name, FieldAttribute::kIndexed>,
    Field<int64_t, age_name, FieldAttribute::kNone>,
    Field<double, balance_name, FieldAttribute::kNone>
>;

// Define record class inheriting from template base
class User : public TypedRecord<UserFields> {
public:
    // Individual field members (pure template approach)
    Field<std::string, id_name, FieldAttribute::kPrimaryKey> id;
    Field<std::string, email_name, FieldAttribute::kIndexed> email;
    Field<int64_t, age_name, FieldAttribute::kNone> age;
    Field<double, balance_name, FieldAttribute::kNone> balance;

    User() = default;

    Status to_record_batch(std::shared_ptr<arrow::RecordBatch>* batch) const override {
        return record_to_batch(*this, batch);
    }

    Status from_record_batch(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index) override {
        return record_from_batch(*this, batch, row_index);
    }

    Status validate() const override {
        return validate_record(*this);
    }

    // Make these public so template functions in record.h can access them
    template<size_t I = 0>
    void set_field_values(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index);

    template<size_t I = 0>
    void collect_field_values(std::vector<std::shared_ptr<arrow::Array>>& arrays) const;
};

// Template specializations for the User record
template<>
template<size_t I>
void User::set_field_values(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index) {
    if constexpr (I < UserFields::size) {
        // Set field I using direct member access (pure template approach)
        if constexpr (I == 0) { // id field
            id.value = get_scalar_value<std::string>(batch->column(I), row_index);
        } else if constexpr (I == 1) { // email field
            email.value = get_scalar_value<std::string>(batch->column(I), row_index);
        } else if constexpr (I == 2) { // age field
            age.value = get_scalar_value<int64_t>(batch->column(I), row_index);
        } else if constexpr (I == 3) { // balance field
            balance.value = get_scalar_value<double>(batch->column(I), row_index);
        }

        // Recursively set next field
        set_field_values<I + 1>(batch, row_index);
    }
}

template<>
template<size_t I>
void User::collect_field_values(std::vector<std::shared_ptr<arrow::Array>>& arrays) const {
    if constexpr (I < UserFields::size) {
        // Collect field I using direct member access (pure template approach)
        if constexpr (I == 0) { // id field
            arrays.push_back(create_arrow_array<std::string>({id.value}));
        } else if constexpr (I == 1) { // email field
            arrays.push_back(create_arrow_array<std::string>({email.value}));
        } else if constexpr (I == 2) { // age field
            arrays.push_back(create_arrow_array<int64_t>({age.value}));
        } else if constexpr (I == 3) { // balance field
            arrays.push_back(create_arrow_array<double>({balance.value}));
        }

        // Recursively collect next field
        collect_field_values<I + 1>(arrays);
    }
}

// NOTE: User struct template specializations removed - User class does not exist
// These were example code that should be implemented per concrete type
// For production use, implement these specializations for your actual record types

} // namespace marble
