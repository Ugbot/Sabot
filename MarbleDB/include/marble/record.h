#pragma once

#include <memory>
#include <vector>
#include <string>
#include <unordered_map>
#include <type_traits>
#include <optional>
#include <marble/status.h>
#include <arrow/api.h>

namespace marble {

class Key;
class RecordRef;

//==============================================================================
// Type-Safe Field System (Inspired by Tonbo's macro system)
//==============================================================================

/**
 * @brief Field attributes for compile-time type safety
 */
enum class FieldAttribute {
    kNone = 0,
    kPrimaryKey = 1 << 0,
    kNullable = 1 << 1,
    kIndexed = 1 << 2,
    kPartitionKey = 1 << 3,
    kSortKey = 1 << 4
};

inline FieldAttribute operator|(FieldAttribute a, FieldAttribute b) {
    return static_cast<FieldAttribute>(static_cast<int>(a) | static_cast<int>(b));
}

inline int operator&(FieldAttribute a, FieldAttribute b) {
    return static_cast<int>(a) & static_cast<int>(b);
}

inline bool operator==(FieldAttribute a, FieldAttribute b) {
    return static_cast<int>(a) == static_cast<int>(b);
}

/**
 * @brief Compile-time type mapping to Arrow types
 */
template<typename T>
struct ArrowTypeMap;

#define MARBLE_DEFINE_ARROW_TYPE(cpp_type, arrow_type_expr) \
template<> \
struct ArrowTypeMap<cpp_type> { \
    static std::shared_ptr<arrow::DataType> type() { return arrow_type_expr; } \
};

MARBLE_DEFINE_ARROW_TYPE(bool, arrow::boolean())
MARBLE_DEFINE_ARROW_TYPE(int8_t, arrow::int8())
MARBLE_DEFINE_ARROW_TYPE(int16_t, arrow::int16())
MARBLE_DEFINE_ARROW_TYPE(int32_t, arrow::int32())
MARBLE_DEFINE_ARROW_TYPE(int64_t, arrow::int64())
MARBLE_DEFINE_ARROW_TYPE(uint8_t, arrow::uint8())
MARBLE_DEFINE_ARROW_TYPE(uint16_t, arrow::uint16())
MARBLE_DEFINE_ARROW_TYPE(uint32_t, arrow::uint32())
MARBLE_DEFINE_ARROW_TYPE(uint64_t, arrow::uint64())
MARBLE_DEFINE_ARROW_TYPE(float, arrow::float32())
MARBLE_DEFINE_ARROW_TYPE(double, arrow::float64())
MARBLE_DEFINE_ARROW_TYPE(std::string, arrow::utf8())

// Nullable types
template<typename T>
struct ArrowTypeMap<std::optional<T>> {
    static std::shared_ptr<arrow::DataType> type() {
        return ArrowTypeMap<T>::type();
    }
};

/**
 * @brief Type-safe field descriptor with compile-time name and attributes
 */
template<typename T, const char* Name, FieldAttribute Attr = FieldAttribute::kNone>
struct Field {
    using value_type = T;
    static constexpr FieldAttribute attributes = Attr;
    static constexpr bool is_primary_key = (static_cast<int>(Attr) & static_cast<int>(FieldAttribute::kPrimaryKey)) != 0;
    static constexpr bool is_nullable = (static_cast<int>(Attr) & static_cast<int>(FieldAttribute::kNullable)) != 0;
    static constexpr bool is_indexed = (static_cast<int>(Attr) & static_cast<int>(FieldAttribute::kIndexed)) != 0;
    static constexpr bool is_partition_key = (static_cast<int>(Attr) & static_cast<int>(FieldAttribute::kPartitionKey)) != 0;
    static constexpr bool is_sort_key = (static_cast<int>(Attr) & static_cast<int>(FieldAttribute::kSortKey)) != 0;
    static constexpr const char* name = Name;

    T value;

    Field() = default;
    Field(T field_value) : value(std::move(field_value)) {}

    // Get the Arrow field definition (compile-time name)
    static std::shared_ptr<arrow::Field> to_arrow_field() {
        return arrow::field(std::string(name), ArrowTypeMap<T>::type(), is_nullable);
    }

    // Get the field name as string
    static std::string field_name() {
        return std::string(name);
    }

    // Get the underlying value (handles optional types)
    const auto& get() const { return value; }
    auto& get() { return value; }
};

/**
 * @brief Compile-time field list using template parameter pack
 */
template<typename... Fields>
struct FieldList {
    static constexpr size_t size = sizeof...(Fields);

    // Convert to Arrow schema at compile time
    static std::shared_ptr<arrow::Schema> to_arrow_schema() {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(size);

        // Use fold expression to collect all fields
        (fields.push_back(Fields::to_arrow_field()), ...);

        return arrow::schema(fields);
    }

    // Get field names (compile-time)
    static std::vector<std::string> field_names() {
        std::vector<std::string> names;
        names.reserve(size);
        (names.push_back(Fields::field_name()), ...);
        return names;
    }

    // Get primary key field names (compile-time)
    static std::vector<std::string> primary_key_fields() {
        std::vector<std::string> keys;
        ((Fields::is_primary_key ? keys.push_back(Fields::field_name()) : void()), ...);
        return keys;
    }

    // Get indexed field names (compile-time)
    static std::vector<std::string> indexed_fields() {
        std::vector<std::string> indexed;
        ((Fields::is_indexed ? indexed.push_back(Fields::field_name()) : void()), ...);
        return indexed;
    }
};

/**
 * @brief Type-safe record base class with template-driven schema
 */
template<typename FieldListType>
class TypedRecord {
protected:
    std::shared_ptr<arrow::Schema> schema_;

public:
    TypedRecord() {
        schema_ = FieldListType::to_arrow_schema();
    }

    virtual ~TypedRecord() = default;

    // Get the Arrow schema (compile-time generated)
    const std::shared_ptr<arrow::Schema>& schema() const {
        return schema_;
    }

    // Get field names (compile-time generated)
    std::vector<std::string> field_names() const {
        return FieldListType::field_names();
    }

    // Get primary key field names (compile-time generated)
    std::vector<std::string> primary_key_fields() const {
        return FieldListType::primary_key_fields();
    }

    // Convert record to Arrow RecordBatch (must be implemented by derived class)
    virtual Status to_record_batch(std::shared_ptr<arrow::RecordBatch>* batch) const = 0;

    // Create record from Arrow RecordBatch (must be implemented by derived class)
    virtual Status from_record_batch(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index) = 0;

    // Validate record constraints (must be implemented by derived class)
    virtual Status validate() const = 0;
};

/**
 * @brief Pure template-based record definition (no macros, no preprocessor)
 *
 * Usage example:
 * ```cpp
 * // Define field name constants (compile-time string literals)
 * constexpr const char id_name[] = "id";
 * constexpr const char email_name[] = "email";
 * constexpr const char age_name[] = "age";
 * constexpr const char balance_name[] = "balance";
 *
 * // Define field list using templates only
 * using UserFields = FieldList<
 *     Field<std::string, id_name, FieldAttribute::kPrimaryKey>,
 *     Field<std::string, email_name, FieldAttribute::kIndexed>,
 *     Field<int64_t, age_name, FieldAttribute::kNone>,
 *     Field<double, balance_name, FieldAttribute::kNone>
 * >;
 *
 * // Define record class inheriting from template base
 * class User : public TypedRecord<UserFields> {
 * public:
 *     UserFields fields;
 *
 *     User() = default;
 *
 *     Status to_record_batch(std::shared_ptr<arrow::RecordBatch>* batch) const override {
 *         return record_to_batch(*this, batch);
 *     }
 *
 *     Status from_record_batch(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index) override {
 *         return record_from_batch(*this, batch, row_index);
 *     }
 *
 *     Status validate() const override {
 *         return validate_record(*this);
 *     }
 *
 * private:
 *     template<size_t I = 0>
 *     void set_field_values(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index);
 *
 *     template<size_t I = 0>
 *     void collect_field_values(std::vector<std::shared_ptr<arrow::Array>>& arrays) const;
 * };
 * ```
 */

//==============================================================================
// Implementation Helpers
//==============================================================================

/**
 * @brief Convert typed record to Arrow RecordBatch
 */
template<typename RecordType>
Status record_to_batch(const RecordType& record, std::shared_ptr<arrow::RecordBatch>* batch) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    record.template collect_field_values<0>(arrays);
    *batch = arrow::RecordBatch::Make(record.schema(), 1, arrays);
    return Status::OK();
}

/**
 * @brief Create typed record from Arrow RecordBatch
 */
template<typename RecordType>
Status record_from_batch(RecordType& record, const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index) {
    if (row_index >= batch->num_rows()) {
        return Status::InvalidArgument("Row index out of bounds");
    }
    record.template set_field_values<0>(batch, row_index);
    return Status::OK();
}

/**
 * @brief Validate record constraints
 */
template<typename RecordType>
Status validate_record(const RecordType& record) {
    // Validate primary key fields are not null/empty
    auto pk_fields = record.primary_key_fields();
    // Implementation would validate primary key constraints
    // This is a placeholder for compile-time validation logic

    return Status::OK();
}

//==============================================================================
// Legacy Record Interface (for backward compatibility)
//==============================================================================

// Abstract base class for records (legacy interface)
class Record {
public:
    virtual ~Record() = default;

    // Get the primary key for this record
    virtual std::shared_ptr<Key> GetKey() const = 0;

    // Convert to Arrow RecordBatch
    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const = 0;

    // Get the Arrow schema for this record type
    virtual std::shared_ptr<arrow::Schema> GetArrowSchema() const = 0;

    // Get a zero-copy reference to this record
    virtual std::unique_ptr<RecordRef> AsRecordRef() const = 0;

    // MVCC versioning support
    virtual void SetMVCCInfo(uint64_t begin_ts, uint64_t commit_ts) = 0;
    virtual uint64_t GetBeginTimestamp() const = 0;
    virtual uint64_t GetCommitTimestamp() const = 0;
    virtual bool IsVisible(uint64_t snapshot_ts) const = 0;
};

/**
 * @brief Type-safe table operations using templates
 */
template<typename RecordType>
class TypedTable {
private:
    std::string table_name_;
    std::shared_ptr<arrow::Schema> schema_;

public:
    TypedTable(std::string table_name)
        : table_name_(std::move(table_name)) {
        RecordType record;
        schema_ = record.schema();
    }

    const std::string& name() const { return table_name_; }
    const std::shared_ptr<arrow::Schema>& schema() const { return schema_; }

    // Type-safe operations would go here
    // - Insert record
    // - Query by primary key
    // - Scan with type-safe predicates
    // - etc.
};

/**
 * @brief Schema registry for managing record schemas
 */
class SchemaRegistry {
private:
    std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> schemas_;

public:
    template<typename RecordType>
    Status register_schema(const std::string& name) {
        RecordType record;
        schemas_[name] = record.schema();
        return Status::OK();
    }

    std::shared_ptr<arrow::Schema> get_schema(const std::string& name) const {
        auto it = schemas_.find(name);
        return it != schemas_.end() ? it->second : nullptr;
    }

    std::vector<std::string> list_schemas() const {
        std::vector<std::string> names;
        names.reserve(schemas_.size());
        for (const auto& pair : schemas_) {
            names.push_back(pair.first);
        }
        return names;
    }
};

// Global schema registry
extern SchemaRegistry global_schema_registry;

/**
 * @brief Record factory for dynamic record creation
 */
class RecordFactory {
public:
    template<typename RecordType>
    static std::unique_ptr<RecordType> create() {
        return std::make_unique<RecordType>();
    }

    template<typename RecordType>
    static std::unique_ptr<RecordType> create_from_batch(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index) {
        auto record = std::make_unique<RecordType>();
        auto status = record->from_record_batch(batch, row_index);
        if (!status.ok()) {
            return nullptr;
        }
        return record;
    }
};

//==============================================================================
// Legacy Record Interface (for backward compatibility)
//==============================================================================

// Zero-copy record reference interface
class RecordRef {
public:
    virtual ~RecordRef() = default;

    // Get the primary key for zero-copy access
    virtual std::shared_ptr<Key> key() const = 0;

    // Get a field value by name (zero-copy where possible)
    virtual arrow::Result<std::shared_ptr<arrow::Scalar>> GetField(const std::string& field_name) const = 0;

    // Get all field values as scalars
    virtual arrow::Result<std::vector<std::shared_ptr<arrow::Scalar>>> GetFields() const = 0;

    // Get the size of this record in bytes
    virtual size_t Size() const = 0;
};

// Abstract base class for keys
class Key {
public:
    virtual ~Key() = default;

    // Compare with another key
    virtual int Compare(const Key& other) const = 0;

    // Convert to Arrow scalar
    virtual arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const = 0;

    // Clone the key
    virtual std::shared_ptr<Key> Clone() const = 0;

    // Get string representation for debugging
    virtual std::string ToString() const = 0;

    // Hash function for unordered containers
    virtual size_t Hash() const = 0;

    bool operator==(const Key& other) const { return Compare(other) == 0; }
    bool operator!=(const Key& other) const { return Compare(other) != 0; }


    bool operator<(const Key& other) const { return Compare(other) < 0; }
    bool operator<=(const Key& other) const { return Compare(other) <= 0; }
    bool operator>(const Key& other) const { return Compare(other) > 0; }
    bool operator>=(const Key& other) const { return Compare(other) >= 0; }
};

//==============================================================================
// Concrete Key Implementations
//==============================================================================

/**
 * @brief Triple key implementation for subject-predicate-object triples
 */
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
        // Return a struct scalar representing the triple
        return arrow::MakeNullScalar(arrow::null());
    }

    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<TripleKey>(subject_, predicate_, object_);
    }

    std::string ToString() const override {
        return std::to_string(subject_) + "," + std::to_string(predicate_) + "," + std::to_string(object_);
    }

    size_t Hash() const override {
        size_t h = 0;
        h = h * 31 + std::hash<int64_t>()(subject_);
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

/**
 * @brief Simple int64 key for benchmarking and testing
 */
class Int64Key : public Key {
public:
    explicit Int64Key(int64_t value) : value_(value) {}

    int Compare(const Key& other) const override {
        const Int64Key* other_key = dynamic_cast<const Int64Key*>(&other);
        if (!other_key) return -1;
        return value_ < other_key->value_ ? -1 : (value_ > other_key->value_ ? 1 : 0);
    }

    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return arrow::MakeScalar(value_);
    }

    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<Int64Key>(value_);
    }

    std::string ToString() const override {
        return std::to_string(value_);
    }

    size_t Hash() const override {
        return std::hash<int64_t>()(value_);
    }

    int64_t value() const { return value_; }

private:
    int64_t value_;
};

// Schema definition for records
class Schema {
public:
    virtual ~Schema() = default;

    // Get the Arrow schema
    virtual std::shared_ptr<arrow::Schema> GetArrowSchema() const = 0;

    // Get primary key indices (for composite keys)
    virtual const std::vector<size_t>& GetPrimaryKeyIndices() const = 0;

    // Create a record from an Arrow RecordBatch row
    virtual arrow::Result<std::shared_ptr<Record>> RecordFromRow(
        const std::shared_ptr<arrow::RecordBatch>& batch, int row_index) const = 0;

    // Create a key from an Arrow scalar
    virtual arrow::Result<std::shared_ptr<Key>> KeyFromScalar(
        const std::shared_ptr<arrow::Scalar>& scalar) const = 0;
};

// Key range for scanning
class KeyRange {
public:
    KeyRange(std::shared_ptr<Key> start, bool start_inclusive,
             std::shared_ptr<Key> end, bool end_inclusive)
        : start_(std::move(start))
        , start_inclusive_(start_inclusive)
        , end_(std::move(end))
        , end_inclusive_(end_inclusive) {}

    static KeyRange All() {
        return KeyRange(nullptr, true, nullptr, true);
    }

    static KeyRange StartAt(std::shared_ptr<Key> start, bool inclusive = true) {
        return KeyRange(std::move(start), inclusive, nullptr, true);
    }

    static KeyRange EndAt(std::shared_ptr<Key> end, bool inclusive = true) {
        return KeyRange(nullptr, true, std::move(end), inclusive);
    }

    const std::shared_ptr<Key>& start() const { return start_; }
    const std::shared_ptr<Key>& end() const { return end_; }
    bool start_inclusive() const { return start_inclusive_; }
    bool end_inclusive() const { return end_inclusive_; }

private:
    std::shared_ptr<Key> start_;
    bool start_inclusive_;
    std::shared_ptr<Key> end_;
    bool end_inclusive_;
};

// Iterator interface for scanning records
class Iterator {
public:
    virtual ~Iterator() = default;

    // Check if the iterator is valid
    virtual bool Valid() const = 0;

    // Move to the next record
    virtual void Next() = 0;

    // Get the current key
    virtual std::shared_ptr<Key> key() const = 0;

    // Get the current record
    virtual std::shared_ptr<Record> value() const = 0;

    // Get zero-copy reference to current record
    virtual std::unique_ptr<RecordRef> value_ref() const = 0;

    // Get the status of the iterator
    virtual marble::Status status() const = 0;

    // Seek to a specific key
    virtual void Seek(const Key& target) = 0;

    // Seek to the first key >= target
    virtual void SeekForPrev(const Key& target) = 0;

    // Seek to the last key
    virtual void SeekToLast() = 0;

    // Move to the previous record (for reverse iteration)
    virtual void Prev() = 0;
};

//==============================================================================
// Simple Record Implementation
//==============================================================================

/**
 * @brief Simple concrete Record implementation that wraps a RecordBatch row
 */
class SimpleRecord : public Record {
public:
    SimpleRecord(std::shared_ptr<Key> key, std::shared_ptr<arrow::RecordBatch> batch, int64_t row_index);

    std::shared_ptr<Key> GetKey() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override;
    std::shared_ptr<arrow::Schema> GetArrowSchema() const override;
    std::unique_ptr<RecordRef> AsRecordRef() const override;

    // MVCC versioning support
    void SetMVCCInfo(uint64_t begin_ts, uint64_t commit_ts) override;
    uint64_t GetBeginTimestamp() const override;
    uint64_t GetCommitTimestamp() const override;
    bool IsVisible(uint64_t snapshot_ts) const override;

private:
    std::shared_ptr<Key> key_;
    std::shared_ptr<arrow::RecordBatch> batch_;
    int64_t row_index_;
    uint64_t begin_ts_;
    uint64_t commit_ts_;
};

} // namespace marble
