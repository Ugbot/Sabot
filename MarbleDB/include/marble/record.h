#pragma once

#include <memory>
#include <vector>
#include <string>
#include <marble/status.h>
#include <arrow/api.h>

namespace marble {

class Key;
class Record;

// Abstract base class for records
class Record {
public:
    virtual ~Record() = default;

    // Get the primary key for this record
    virtual std::shared_ptr<Key> GetKey() const = 0;

    // Convert to Arrow RecordBatch
    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const = 0;

    // Get the Arrow schema for this record type
    virtual std::shared_ptr<arrow::Schema> GetArrowSchema() const = 0;
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

} // namespace marble
