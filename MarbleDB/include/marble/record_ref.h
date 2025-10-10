/************************************************************************
MarbleDB Zero-Copy RecordRef System
Inspired by Tonbo's zero-allocation record references

Provides zero-copy access to records stored in Arrow RecordBatch,
avoiding deserialization overhead and reducing memory usage by 10-100x.
**************************************************************************/

#pragma once

#include <marble/status.h>
#include <arrow/api.h>
#include <memory>
#include <string>
#include <string_view>
#include <optional>
#include <vector>

namespace marble {

// Forward declarations
class Key;

/**
 * @brief Zero-copy reference to a record field value
 * 
 * Holds a reference to data within an Arrow array without copying.
 * Lifetime is tied to the underlying RecordBatch.
 */
class FieldRef {
public:
    enum class Type {
        kNull,
        kInt64,
        kDouble,
        kString,
        kBinary,
        kBoolean
    };

    FieldRef() : type_(Type::kNull) {}
    
    // Construct from Arrow array at offset
    static FieldRef FromArray(const arrow::Array& array, size_t offset);
    
    // Type checking
    Type type() const { return type_; }
    bool is_null() const { return type_ == Type::kNull; }
    
    // Zero-copy accessors (throw if wrong type)
    std::optional<int64_t> as_int64() const;
    std::optional<double> as_double() const;
    std::optional<std::string_view> as_string() const;
    std::optional<std::string_view> as_binary() const;
    std::optional<bool> as_boolean() const;

private:
    Type type_;
    
    // Value storage (lightweight - just pointers/scalars)
    union {
        int64_t int64_val;
        double double_val;
        struct {
            const char* data;
            size_t length;
        } string_val;
        bool bool_val;
    };
};

/**
 * @brief Zero-copy reference to a record in a RecordBatch
 * 
 * Provides field access without materializing the entire record.
 * Inspired by Tonbo's RecordRef trait.
 * 
 * Example:
 *   ArrowRecordRef ref(batch, row_index, projection);
 *   auto name = ref.get_string("name");  // Zero-copy string_view
 *   auto age = ref.get_int64("age");     // Direct i64 access
 */
class ArrowRecordRef {
public:
    /**
     * @brief Construct a zero-copy reference to a record
     * 
     * @param batch The RecordBatch containing the record
     * @param offset Row index within the batch
     * @param projection Optional projection mask (NULL = all columns)
     */
    ArrowRecordRef(const std::shared_ptr<arrow::RecordBatch>& batch,
                   size_t offset,
                   const std::shared_ptr<arrow::Schema>& full_schema);
    
    /**
     * @brief Get field value by name (zero-copy)
     */
    FieldRef get(const std::string& field_name) const;
    
    /**
     * @brief Type-specific accessors (convenience)
     */
    std::optional<int64_t> get_int64(const std::string& field) const;
    std::optional<double> get_double(const std::string& field) const;
    std::optional<std::string_view> get_string(const std::string& field) const;
    std::optional<std::string_view> get_binary(const std::string& field) const;
    std::optional<bool> get_boolean(const std::string& field) const;
    
    /**
     * @brief Extract primary key (zero-copy)
     */
    std::shared_ptr<Key> key() const;
    
    /**
     * @brief Check if record is a tombstone (deleted)
     */
    bool is_tombstone() const;
    
    /**
     * @brief Get timestamp of this record version
     */
    uint32_t timestamp() const;
    
    /**
     * @brief Number of fields in this record
     */
    size_t field_count() const { return full_schema_->num_fields(); }
    
    /**
     * @brief Get field names
     */
    std::vector<std::string> field_names() const;
    
    /**
     * @brief Underlying RecordBatch (for advanced use)
     */
    const arrow::RecordBatch* record_batch() const { return batch_.get(); }
    size_t offset() const { return offset_; }

private:
    std::shared_ptr<arrow::RecordBatch> batch_;
    size_t offset_;
    std::shared_ptr<arrow::Schema> full_schema_;
    
    // Cached column lookups
    mutable std::unordered_map<std::string, int> field_index_cache_;
    
    int field_index(const std::string& name) const;
};

/**
 * @brief Owned key reference (lightweight copy)
 * 
 * Can be extracted from RecordRef and used independently.
 */
class KeyRef {
public:
    virtual ~KeyRef() = default;
    
    // Comparison for ordering
    virtual int Compare(const KeyRef& other) const = 0;
    
    // String representation
    virtual std::string ToString() const = 0;
    
    // Convert to owned Key
    virtual std::shared_ptr<Key> ToKey() const = 0;
};

/**
 * @brief Iterator over ArrowRecordRefs in a RecordBatch
 * 
 * Allows zero-copy iteration without materializing records.
 */
class RecordBatchIterator {
public:
    RecordBatchIterator(const std::shared_ptr<arrow::RecordBatch>& batch,
                       const std::shared_ptr<arrow::Schema>& schema)
        : batch_(batch), schema_(schema), offset_(0) {}
    
    bool HasNext() const { return offset_ < static_cast<size_t>(batch_->num_rows()); }
    
    ArrowRecordRef Next() {
        if (!HasNext()) {
            throw std::runtime_error("Iterator exhausted");
        }
        return ArrowRecordRef(batch_, offset_++, schema_);
    }
    
    void Reset() { offset_ = 0; }

private:
    std::shared_ptr<arrow::RecordBatch> batch_;
    std::shared_ptr<arrow::Schema> schema_;
    size_t offset_;
};

/**
 * @brief Optional ArrowRecordRef (handles tombstones)
 * 
 * Similar to Tonbo's OptionRecordRef.
 */
class OptionArrowRecordRef {
public:
    OptionArrowRecordRef(const ArrowRecordRef& ref, bool is_null, uint32_t ts)
        : ref_(ref), is_null_(is_null), ts_(ts) {}
    
    bool has_value() const { return !is_null_; }
    const ArrowRecordRef& value() const { 
        if (is_null_) throw std::runtime_error("Accessing null record");
        return ref_; 
    }
    
    uint32_t timestamp() const { return ts_; }
    std::shared_ptr<Key> key() const { return ref_.key(); }

private:
    ArrowRecordRef ref_;
    bool is_null_;
    uint32_t ts_;
};

} // namespace marble

