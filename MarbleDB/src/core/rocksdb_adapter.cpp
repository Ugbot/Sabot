/**
 * RocksDB Compatibility Layer Implementation
 *
 * Adapts RocksDB API calls to MarbleDB's internal implementation.
 */

#include "marble/rocksdb_compat.h"
#include "marble/api.h"
#include "marble/record.h"
#include <iostream>

namespace marble {
namespace rocksdb {

//==============================================================================
// WriteBatch Implementation
//==============================================================================

WriteBatch::WriteBatch() = default;
WriteBatch::~WriteBatch() = default;

void WriteBatch::Put(const Slice& key, const Slice& value) {
    Put(nullptr, key, value);
}

void WriteBatch::Put(ColumnFamilyHandle* cf, const Slice& key, const Slice& value) {
    operations_.push_back({
        Operation::PUT,
        cf,
        std::string(key),
        std::string(value)
    });
}

void WriteBatch::Delete(const Slice& key) {
    Delete(nullptr, key);
}

void WriteBatch::Delete(ColumnFamilyHandle* cf, const Slice& key) {
    operations_.push_back({
        Operation::DELETE,
        cf,
        std::string(key),
        std::string()
    });
}

void WriteBatch::Merge(const Slice& key, const Slice& value) {
    Merge(nullptr, key, value);
}

void WriteBatch::Merge(ColumnFamilyHandle* cf, const Slice& key, const Slice& value) {
    operations_.push_back({
        Operation::MERGE,
        cf,
        std::string(key),
        std::string(value)
    });
}

void WriteBatch::Clear() {
    operations_.clear();
}

int WriteBatch::Count() const {
    return static_cast<int>(operations_.size());
}

//==============================================================================
// Iterator Implementation
//==============================================================================

class RocksDBIteratorAdapter : public Iterator {
public:
    explicit RocksDBIteratorAdapter(std::unique_ptr<marble::Iterator> impl)
        : impl_(std::move(impl))
        , current_key_()
        , current_value_() {}

    bool Valid() const override {
        return impl_->Valid();
    }

    void SeekToFirst() override {
        impl_->Seek(*std::make_shared<marble::Int64Key>(0));
        // TODO: Implement proper SeekToFirst in marble::Iterator
    }

    void SeekToLast() override {
        impl_->SeekToLast();
    }

    void Seek(const Slice& target) override {
        // Convert Slice to Key
        // For simplicity, hash the string to uint64_t
        uint64_t key_hash = std::hash<std::string>{}(std::string(target));
        auto key = std::make_shared<marble::Int64Key>(key_hash);
        impl_->Seek(*key);
    }

    void SeekForPrev(const Slice& target) override {
        uint64_t key_hash = std::hash<std::string>{}(std::string(target));
        auto key = std::make_shared<marble::Int64Key>(key_hash);
        impl_->SeekForPrev(*key);
    }

    void Next() override {
        impl_->Next();
    }

    void Prev() override {
        impl_->Prev();
    }

    Slice key() const override {
        if (!impl_->Valid()) return Slice();

        // Get the record and extract key
        auto record = impl_->value();
        if (!record) return Slice();

        auto key_ptr = record->GetKey();
        if (!key_ptr) return Slice();

        // Cache the key string
        current_key_ = std::to_string(key_ptr->Hash());
        return Slice(current_key_);
    }

    Slice value() const override {
        if (!impl_->Valid()) return Slice();

        // Get the record
        auto record = impl_->value();
        if (!record) return Slice();

        // Convert record to RecordBatch and extract value
        auto batch_result = record->ToRecordBatch();
        if (!batch_result.ok()) return Slice();

        auto batch = batch_result.ValueOrDie();
        if (batch->num_rows() == 0 || batch->num_columns() < 2) return Slice();

        // Assume value is in second column (column 1)
        auto value_array = batch->column(1);
        if (!value_array || value_array->length() == 0) return Slice();

        // Extract first value as binary
        if (value_array->type()->id() == arrow::Type::BINARY) {
            auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(value_array);
            if (binary_array->IsValid(0)) {
                int32_t length;
                const uint8_t* data = binary_array->GetValue(0, &length);
                current_value_ = std::string(reinterpret_cast<const char*>(data), length);
                return Slice(current_value_);
            }
        }

        return Slice();
    }

    marble::Status status() const override {
        return impl_->status();
    }

private:
    std::unique_ptr<marble::Iterator> impl_;
    mutable std::string current_key_;
    mutable std::string current_value_;
};

Iterator::~Iterator() = default;

//==============================================================================
// DB Implementation
//==============================================================================

DB::~DB() = default;

marble::Status DB::Open(const Options& options, const std::string& name, DB** dbptr) {
    // Create MarbleDB instance
    std::unique_ptr<marble::MarbleDB> db;
    auto status = marble::OpenDatabase(name, &db);
    if (!status.ok()) {
        return marble::Status::IOError("Failed to open database: " + name);
    }

    // Create RocksDB adapter
    auto rocksdb_adapter = new DB();
    rocksdb_adapter->impl_ = std::move(db);

    // Create and cache default schema
    rocksdb_adapter->default_schema_ = arrow::schema({
        arrow::field("key", arrow::uint64()),
        arrow::field("value", arrow::binary())
    });

    // Create default column family (like RocksDB does automatically)
    marble::TableSchema table_schema;
    table_schema.table_name = "default";
    table_schema.arrow_schema = rocksdb_adapter->default_schema_;

    // Try to create the default table - ignore error if it already exists
    auto create_status = rocksdb_adapter->impl_->CreateTable(table_schema);
    // Ignore if table already exists

    *dbptr = rocksdb_adapter;
    return marble::Status::OK();
}

marble::Status DB::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
    return Put(options, nullptr, key, value);
}

marble::Status DB::Put(const WriteOptions& options, ColumnFamilyHandle* cf,
                       const Slice& key, const Slice& value) {
    // Convert Slice to Record
    // Create a simple key-value record
    uint64_t key_hash = std::hash<std::string>{}(std::string(key));
    auto key_ptr = std::make_shared<marble::Int64Key>(key_hash);

    // Use cached schema (avoiding repeated allocations)
    auto schema = default_schema_;

    // Build arrays
    arrow::UInt64Builder key_builder;
    auto append_key_status = key_builder.Append(key_hash);
    if (!append_key_status.ok()) {
        return marble::Status::InternalError("Failed to append key");
    }

    arrow::BinaryBuilder value_builder;
    auto append_value_status = value_builder.Append(value.data(), value.size());
    if (!append_value_status.ok()) {
        return marble::Status::InternalError("Failed to append value");
    }

    std::shared_ptr<arrow::Array> key_array;
    auto finish_key_status = key_builder.Finish(&key_array);
    if (!finish_key_status.ok()) {
        return marble::Status::InternalError("Failed to finish key array");
    }

    std::shared_ptr<arrow::Array> value_array;
    auto finish_value_status = value_builder.Finish(&value_array);
    if (!finish_value_status.ok()) {
        return marble::Status::InternalError("Failed to finish value array");
    }

    // Create RecordBatch
    auto batch = arrow::RecordBatch::Make(schema, 1, {key_array, value_array});

    // Create Record
    auto record = std::make_shared<marble::SimpleRecord>(key_ptr, batch, 0);

    // Write to MarbleDB
    marble::WriteOptions marble_options;
    return impl_->Put(marble_options, record);
}

marble::Status DB::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    return Get(options, nullptr, key, value);
}

marble::Status DB::Get(const ReadOptions& options, ColumnFamilyHandle* cf,
                       const Slice& key, std::string* value) {
    // Convert key
    uint64_t key_hash = std::hash<std::string>{}(std::string(key));
    auto key_ptr = std::make_shared<marble::Int64Key>(key_hash);

    // Read from MarbleDB
    marble::ReadOptions marble_options;
    std::shared_ptr<marble::Record> record;
    auto status = impl_->Get(marble_options, *key_ptr, &record);

    if (!status.ok()) {
        return status;
    }

    if (!record) {
        return marble::Status::NotFound("Key not found");
    }

    // Extract value from record
    auto batch_result = record->ToRecordBatch();
    if (!batch_result.ok()) {
        return marble::Status::InternalError("Failed to convert record to batch");
    }

    auto batch = batch_result.ValueOrDie();
    if (batch->num_rows() == 0 || batch->num_columns() < 2) {
        return marble::Status::InternalError("Invalid record format");
    }

    // Get value column (column 1)
    auto value_array = batch->column(1);
    if (value_array->type()->id() == arrow::Type::BINARY) {
        auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(value_array);
        if (binary_array->IsValid(0)) {
            int32_t length;
            const uint8_t* data = binary_array->GetValue(0, &length);
            *value = std::string(reinterpret_cast<const char*>(data), length);
            return marble::Status::OK();
        }
    }

    return marble::Status::InternalError("Failed to extract value");
}

marble::Status DB::Delete(const WriteOptions& options, const Slice& key) {
    return Delete(options, nullptr, key);
}

marble::Status DB::Delete(const WriteOptions& options, ColumnFamilyHandle* cf,
                          const Slice& key) {
    uint64_t key_hash = std::hash<std::string>{}(std::string(key));
    auto key_ptr = std::make_shared<marble::Int64Key>(key_hash);

    marble::WriteOptions marble_options;
    return impl_->Delete(marble_options, *key_ptr);
}

marble::Status DB::Merge(const WriteOptions& options, const Slice& key, const Slice& value) {
    return Merge(options, nullptr, key, value);
}

marble::Status DB::Merge(const WriteOptions& options, ColumnFamilyHandle* cf,
                         const Slice& key, const Slice& value) {
    // MarbleDB doesn't have direct Merge support in the simple API
    // Fall back to Read-Modify-Write
    std::string existing_value;
    auto get_status = Get(ReadOptions(), cf, key, &existing_value);

    // If key doesn't exist, just put the new value
    if (!get_status.ok()) {
        return Put(options, cf, key, value);
    }

    // Simple concatenation merge (could be customized)
    std::string merged = existing_value + std::string(value);
    return Put(options, cf, key, merged);
}

marble::Status DB::Write(const WriteOptions& options, WriteBatch* batch) {
    if (!batch) {
        return marble::Status::InvalidArgument("Null batch");
    }

    if (batch->operations_.empty()) {
        return marble::Status::OK();
    }

    // ★★★ OPTIMIZATION: Use InsertBatch for bulk PUT operations ★★★
    // Separate operations by type
    std::vector<const WriteBatch::Operation*> puts;
    std::vector<const WriteBatch::Operation*> deletes;
    std::vector<const WriteBatch::Operation*> merges;

    for (const auto& op : batch->operations_) {
        switch (op.type) {
            case WriteBatch::Operation::PUT:
                puts.push_back(&op);
                break;
            case WriteBatch::Operation::DELETE:
                deletes.push_back(&op);
                break;
            case WriteBatch::Operation::MERGE:
                merges.push_back(&op);
                break;
        }
    }

    // Process PUTs in bulk using InsertBatch
    if (!puts.empty()) {
        // Build arrays for all PUT operations at once
        arrow::UInt64Builder key_builder;
        arrow::BinaryBuilder value_builder;

        for (const auto* op : puts) {
            uint64_t key_hash = std::hash<std::string>{}(op->key);
            auto append_key_status = key_builder.Append(key_hash);
            if (!append_key_status.ok()) {
                return marble::Status::InternalError("Failed to append key");
            }

            auto append_value_status = value_builder.Append(op->value);
            if (!append_value_status.ok()) {
                return marble::Status::InternalError("Failed to append value");
            }
        }

        // Finish arrays
        std::shared_ptr<arrow::Array> key_array;
        auto finish_key_status = key_builder.Finish(&key_array);
        if (!finish_key_status.ok()) {
            return marble::Status::InternalError("Failed to finish key array");
        }

        std::shared_ptr<arrow::Array> value_array;
        auto finish_value_status = value_builder.Finish(&value_array);
        if (!finish_value_status.ok()) {
            return marble::Status::InternalError("Failed to finish value array");
        }

        // Create single RecordBatch for all PUTs
        auto record_batch = arrow::RecordBatch::Make(
            default_schema_,
            puts.size(),
            {key_array, value_array}
        );

        // Use InsertBatch API for bulk insert
        auto insert_status = impl_->InsertBatch("default", record_batch);
        if (!insert_status.ok()) {
            return insert_status;
        }
    }

    // Process DELETEs individually (less common)
    for (const auto* op : deletes) {
        auto status = Delete(options, op->cf, op->key);
        if (!status.ok()) {
            return status;
        }
    }

    // Process MERGEs individually (less common)
    for (const auto* op : merges) {
        auto status = Merge(options, op->cf, op->key, op->value);
        if (!status.ok()) {
            return status;
        }
    }

    return marble::Status::OK();
}

std::vector<marble::Status> DB::MultiGet(const ReadOptions& options,
                                          const std::vector<Slice>& keys,
                                          std::vector<std::string>* values) {
    return MultiGet(options, nullptr, keys, values);
}

std::vector<marble::Status> DB::MultiGet(const ReadOptions& options,
                                          ColumnFamilyHandle* cf,
                                          const std::vector<Slice>& keys,
                                          std::vector<std::string>* values) {
    std::vector<marble::Status> statuses;
    values->resize(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        auto status = Get(options, cf, keys[i], &(*values)[i]);
        statuses.push_back(status);
    }

    return statuses;
}

Iterator* DB::NewIterator(const ReadOptions& options) {
    return NewIterator(options, nullptr);
}

Iterator* DB::NewIterator(const ReadOptions& options, ColumnFamilyHandle* cf) {
    marble::ReadOptions marble_options;
    marble::KeyRange range = marble::KeyRange::All();  // Full range

    std::unique_ptr<marble::Iterator> marble_iter;
    auto status = impl_->NewIterator(marble_options, range, &marble_iter);

    if (!status.ok() || !marble_iter) {
        // Return empty iterator
        return nullptr;
    }

    return new RocksDBIteratorAdapter(std::move(marble_iter));
}

marble::Status DB::CreateColumnFamily(const ColumnFamilyOptions& options,
                                       const std::string& name,
                                       ColumnFamilyHandle** handle) {
    // Create table with default schema
    auto schema = arrow::schema({
        arrow::field("key", arrow::uint64()),
        arrow::field("value", arrow::binary())
    });

    marble::TableSchema table_schema;
    table_schema.table_name = name;
    table_schema.arrow_schema = schema;

    return impl_->CreateTable(table_schema);
}

marble::Status DB::DropColumnFamily(ColumnFamilyHandle* handle) {
    if (!handle) {
        return marble::Status::InvalidArgument("Null handle");
    }
    // TODO: Implement DropTable in MarbleDB
    return marble::Status::NotImplemented("DropColumnFamily not yet implemented");
}

std::vector<std::string> DB::ListColumnFamilies() const {
    return impl_->ListColumnFamilies();
}

marble::Status DB::Flush(const FlushOptions& options) {
    return impl_->Flush();
}

marble::Status DB::Flush(const FlushOptions& options, ColumnFamilyHandle* cf) {
    // TODO: Per-CF flush
    return impl_->Flush();
}

marble::Status DB::CompactRange(const CompactRangeOptions& options,
                                 ColumnFamilyHandle* cf,
                                 const Slice* begin,
                                 const Slice* end) {
    // Convert slices to KeyRange
    std::shared_ptr<marble::Key> start_key = nullptr;
    std::shared_ptr<marble::Key> end_key = nullptr;

    if (begin) {
        uint64_t begin_hash = std::hash<std::string>{}(std::string(*begin));
        start_key = std::make_shared<marble::Int64Key>(begin_hash);
    }
    if (end) {
        uint64_t end_hash = std::hash<std::string>{}(std::string(*end));
        end_key = std::make_shared<marble::Int64Key>(end_hash);
    }

    marble::KeyRange range(start_key, true, end_key, true);
    return impl_->CompactRange(range);
}

bool DB::GetProperty(const Slice& property, std::string* value) {
    return GetProperty(nullptr, property, value);
}

bool DB::GetProperty(ColumnFamilyHandle* cf, const Slice& property, std::string* value) {
    // TODO: Implement property queries
    if (property == "rocksdb.estimate-num-keys") {
        *value = "0";  // Placeholder
        return true;
    }
    return false;
}

const void* DB::GetSnapshot() {
    // TODO: Implement snapshot support
    return nullptr;
}

void DB::ReleaseSnapshot(const void* snapshot) {
    // TODO: Implement snapshot support
}

}  // namespace rocksdb
}  // namespace marble
