/************************************************************************
MarbleDB Dynamic Schema System
Inspired by Tonbo's DynRecord for runtime schema definition

Enables:
- Schema evolution without recompilation
- Plugin systems with custom schemas
- Multi-tenant with per-tenant schemas
**************************************************************************/

#pragma once

#include <marble/status.h>
#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>
#include <variant>

namespace marble {

/**
 * @brief Dynamic field descriptor
 * 
 * Describes a field in a dynamic schema.
 */
struct DynamicField {
    std::string name;
    arrow::Type::type data_type;
    bool nullable;
    
    DynamicField(const std::string& n, arrow::Type::type dt, bool null)
        : name(n), data_type(dt), nullable(null) {}
    
    /**
     * @brief Convert to Arrow field
     */
    std::shared_ptr<arrow::Field> ToArrowField() const;
};

/**
 * @brief Dynamic value (variant type)
 * 
 * Can hold any supported data type at runtime.
 * Similar to Tonbo's Value enum.
 */
class DynValue {
public:
    using ValueVariant = std::variant<
        std::monostate,  // Null
        bool,
        int8_t,
        int16_t,
        int32_t,
        int64_t,
        uint8_t,
        uint16_t,
        uint32_t,
        uint64_t,
        float,
        double,
        std::string,
        std::vector<uint8_t>  // Binary
    >;
    
    DynValue() : value_(std::monostate{}) {}
    
    template<typename T>
    explicit DynValue(T val) : value_(std::move(val)) {}
    
    /**
     * @brief Type checking
     */
    bool is_null() const { return std::holds_alternative<std::monostate>(value_); }
    
    arrow::Type::type arrow_type() const;
    
    /**
     * @brief Type-safe accessors
     */
    template<typename T>
    std::optional<T> as() const {
        if (std::holds_alternative<T>(value_)) {
            return std::get<T>(value_);
        }
        return std::nullopt;
    }
    
    std::optional<int64_t> as_int64() const { return as<int64_t>(); }
    std::optional<double> as_double() const { return as<double>(); }
    std::optional<std::string> as_string() const { return as<std::string>(); }
    std::optional<bool> as_bool() const { return as<bool>(); }
    
    /**
     * @brief Convert to Arrow scalar
     */
    std::shared_ptr<arrow::Scalar> ToArrowScalar() const;
    
    /**
     * @brief Compare values
     */
    int Compare(const DynValue& other) const;

private:
    ValueVariant value_;
};

/**
 * @brief Dynamic schema definition
 * 
 * Runtime-defined schema similar to Tonbo's DynSchema.
 */
class DynSchema {
public:
    /**
     * @brief Create dynamic schema
     * 
     * @param fields Field descriptors
     * @param primary_key_index Index of primary key field
     */
    DynSchema(const std::vector<DynamicField>& fields, size_t primary_key_index);
    
    /**
     * @brief Create from Arrow schema
     */
    static std::shared_ptr<DynSchema> FromArrowSchema(
        const arrow::Schema& schema,
        size_t primary_key_index
    );
    
    /**
     * @brief Get Arrow schema
     */
    std::shared_ptr<arrow::Schema> arrow_schema() const { return arrow_schema_; }
    
    /**
     * @brief Field accessors
     */
    const std::vector<DynamicField>& fields() const { return fields_; }
    size_t primary_key_index() const { return primary_key_index_; }
    size_t num_fields() const { return fields_.size(); }
    
    /**
     * @brief Get field by name
     */
    const DynamicField* GetField(const std::string& name) const;

private:
    std::vector<DynamicField> fields_;
    size_t primary_key_index_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
};

/**
 * @brief Dynamic record
 * 
 * Record with runtime-defined schema.
 * Similar to Tonbo's DynRecord.
 */
class DynRecord {
public:
    /**
     * @brief Create dynamic record
     * 
     * @param schema Schema definition
     * @param values Field values (must match schema)
     */
    DynRecord(std::shared_ptr<DynSchema> schema, std::vector<DynValue> values);
    
    /**
     * @brief Validate record against schema
     */
    Status Validate() const;
    
    /**
     * @brief Field access
     */
    const DynValue& GetField(size_t index) const;
    const DynValue& GetField(const std::string& name) const;
    
    /**
     * @brief Primary key
     */
    const DynValue& PrimaryKey() const {
        return values_[schema_->primary_key_index()];
    }
    
    /**
     * @brief Schema
     */
    std::shared_ptr<DynSchema> schema() const { return schema_; }
    
    /**
     * @brief Number of fields
     */
    size_t num_fields() const { return values_.size(); }
    
    /**
     * @brief Convert to Arrow RecordBatch (single row)
     */
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToArrowBatch() const;
    
    /**
     * @brief Create from Arrow RecordBatch row
     */
    static arrow::Result<std::shared_ptr<DynRecord>> FromArrowBatch(
        std::shared_ptr<DynSchema> schema,
        const arrow::RecordBatch& batch,
        size_t row_index
    );

private:
    std::shared_ptr<DynSchema> schema_;
    std::vector<DynValue> values_;
};

/**
 * @brief Dynamic record builder
 * 
 * Efficiently builds multiple DynRecords into Arrow RecordBatch.
 */
class DynRecordBatchBuilder {
public:
    explicit DynRecordBatchBuilder(std::shared_ptr<DynSchema> schema);
    
    /**
     * @brief Add record to batch
     */
    Status Append(const DynRecord& record);
    
    /**
     * @brief Build final RecordBatch
     */
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Finish();
    
    /**
     * @brief Reset builder
     */
    void Reset();
    
    size_t num_records() const { return num_records_; }

private:
    std::shared_ptr<DynSchema> schema_;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders_;
    size_t num_records_;
};

/**
 * @brief Helper macro for creating dynamic schemas
 * 
 * Example:
 *   auto schema = MARBLE_DYN_SCHEMA(
 *       ("id", arrow::int64(), false),
 *       ("name", arrow::utf8(), false),
 *       ("age", arrow::int32(), true),
 *       0  // primary_key_index
 *   );
 */
#define MARBLE_DYN_FIELD(name, type, nullable) \
    marble::DynamicField(name, type, nullable)

} // namespace marble

