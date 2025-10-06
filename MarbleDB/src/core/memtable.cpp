#include <marble/lsm_tree.h>
#include <marble/file_system.h>
#include <arrow/api.h>
#include <arrow/util/key_value_metadata.h>
#include <algorithm>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace marble {


// Forward declaration
class ImmutableSkipListMemTable;

// SkipList-based MemTable implementation
class SkipListMemTable : public MemTable {
public:
    // Iterator nested class
    class SkipListIterator : public MemTable::Iterator {
    public:
        explicit SkipListIterator(const std::map<std::string, std::pair<std::shared_ptr<marble::Key>, std::shared_ptr<Record>>>& data)
            : data_(data)
            , it_(data_.begin()) {}

        bool Valid() const override {
            return it_ != data_.end();
        }

        void SeekToFirst() override {
            it_ = data_.begin();
        }

        void SeekToLast() override {
            if (!data_.empty()) {
                it_ = std::prev(data_.end());
            } else {
                it_ = data_.end();
            }
        }

        void Seek(const marble::Key& key) override {
            it_ = data_.lower_bound(key.ToString());
        }

        void Next() override {
            if (it_ != data_.end()) {
                ++it_;
            }
        }

        void Prev() override {
            if (it_ != data_.begin()) {
                --it_;
            } else {
                it_ = data_.end();
            }
        }

        std::shared_ptr<marble::Key> GetKey() const override {
            if (Valid()) {
                return it_->second.first;
            }
            return nullptr;
        }

        std::shared_ptr<Record> Value() const override {
            if (Valid()) {
                return it_->second.second;
            }
            return nullptr;
        }

        std::unique_ptr<RecordRef> ValueRef() const override {
            // Return zero-copy reference if available
            if (Valid() && it_->second.second) {
                return it_->second.second->AsRecordRef();
            }
            return nullptr;
        }

        marble::Status GetStatus() const override {
            return Status::OK();
        }

    private:
        const std::map<std::string, std::pair<std::shared_ptr<marble::Key>, std::shared_ptr<Record>>>& data_;
        std::map<std::string, std::pair<std::shared_ptr<marble::Key>, std::shared_ptr<Record>>>::const_iterator it_;
    };

public:
    explicit SkipListMemTable(std::shared_ptr<Schema> schema, size_t max_size = 64 * 1024 * 1024)
        : schema_(std::move(schema))
        , max_size_(max_size)
        , current_size_(0) {}

    Status Put(std::shared_ptr<Key> key, std::shared_ptr<Record> record) override {
        std::unique_lock<std::mutex> lock(mutex_);

        // Check if we need to make space
        if (current_size_ >= max_size_) {
            return Status::ResourceExhausted("MemTable is full");
        }

        // Store the record
        data_[key->ToString()] = std::make_pair(key, record);

        // Update size estimate (rough approximation)
        current_size_ += key->ToString().size() + 100;  // rough record size

        return Status::OK();
    }

    Status Get(const Key& key, std::shared_ptr<Record>* record) const override {
        std::unique_lock<std::mutex> lock(mutex_);

        auto it = data_.find(key.ToString());
        if (it == data_.end()) {
            return Status::NotFound("Key not found in MemTable");
        }

        *record = it->second.second;
        return Status::OK();
    }

    Status Delete(std::shared_ptr<Key> key) override {
        std::unique_lock<std::mutex> lock(mutex_);

        auto it = data_.find(key->ToString());
        if (it != data_.end()) {
            data_.erase(it);
        }

        return Status::OK();
    }

    bool IsFull() const override {
        std::unique_lock<std::mutex> lock(mutex_);
        return current_size_ >= max_size_;
    }

    size_t ApproximateMemoryUsage() const override {
        std::unique_lock<std::mutex> lock(mutex_);
        return current_size_;
    }

    size_t Size() const override {
        std::unique_lock<std::mutex> lock(mutex_);
        return data_.size();
    }

    std::unique_ptr<ImmutableMemTable> Freeze() override;

    void Clear() override {
        std::unique_lock<std::mutex> lock(mutex_);
        data_.clear();
        current_size_ = 0;
    }

    std::unique_ptr<Iterator> NewIterator() override {
        std::unique_lock<std::mutex> lock(mutex_);
        return std::make_unique<SkipListIterator>(data_);
    }

private:
    std::shared_ptr<Schema> schema_;
    size_t max_size_;
    mutable std::mutex mutex_;
    std::map<std::string, std::pair<std::shared_ptr<Key>, std::shared_ptr<Record>>> data_;
    size_t current_size_;
};

// Immutable version of the skip list memtable
class ImmutableSkipListMemTable : public ImmutableMemTable {
public:
    explicit ImmutableSkipListMemTable(std::shared_ptr<Schema> schema)
        : ImmutableMemTable()
        , schema_(std::move(schema)) {}

    Status Get(const marble::Key& key, std::shared_ptr<Record>* record) const override {
        auto it = data_.find(key.ToString());
        if (it == data_.end()) {
            return Status::NotFound("Key not found in immutable MemTable");
        }

        *record = it->second.second;
        return Status::OK();
    }

    std::unique_ptr<MemTable::Iterator> NewIterator() override {
        return std::make_unique<SkipListMemTable::SkipListIterator>(data_);
    }

    size_t ApproximateMemoryUsage() const override {
        size_t size = 0;
        for (const auto& pair : data_) {
            size += pair.first.size() + 100;  // rough estimate
        }
        return size;
    }

    size_t Size() const override {
        return data_.size();
    }

    Status ToRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch) const override {
        if (data_.empty()) {
            return Status::InvalidArgument("Cannot create RecordBatch from empty MemTable");
        }

        // Get schema
        auto arrow_schema = schema_->GetArrowSchema();
        if (!arrow_schema) {
            return Status::InvalidArgument("Schema does not support Arrow conversion");
        }

        // Create builders for each field
        std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
        for (const auto& field : arrow_schema->fields()) {
            auto builder_result = arrow::MakeBuilder(field->type(), arrow::default_memory_pool());
            if (!builder_result.ok()) {
                return Status::InvalidArgument("Failed to create builder for field: " + field->name());
            }
            builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(builder_result.ValueUnsafe().release()));
        }

        // Fill builders with data
        for (const auto& pair : data_) {
            auto record = pair.second.second;
            auto record_batch_result = record->ToRecordBatch();
            if (!record_batch_result.ok()) {
                return Status::InvalidArgument("Failed to convert record to batch");
            }

            auto record_batch = record_batch_result.ValueUnsafe();
            if (record_batch->num_rows() != 1) {
                return Status::InvalidArgument("Record must produce exactly one row");
            }

            // Copy data from record batch to our builders
            for (size_t i = 0; i < builders.size(); ++i) {
                auto column = record_batch->column(static_cast<int>(i));
                if (column->length() > 0) {
                    auto scalar_result = column->GetScalar(0);
                    if (scalar_result.ok()) {
                        // This is simplified - real implementation would need to handle different types
                        // For now, we'll skip this complex logic
                    }
                }
            }
        }

        // Build arrays
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (auto& builder : builders) {
            auto result = builder->Finish();
            if (!result.ok()) {
                return Status::InvalidArgument("Failed to finish array builder");
            }
            arrays.push_back(result.ValueUnsafe());
        }

        // Create record batch
        *batch = arrow::RecordBatch::Make(arrow_schema, static_cast<int64_t>(arrays[0]->length()), arrays);
        return Status::OK();
    }

    // Add data (used during freeze operation)
    void AddData(std::map<std::string, std::pair<std::shared_ptr<Key>, std::shared_ptr<Record>>> data) {
        data_ = std::move(data);
    }

private:
    std::shared_ptr<Schema> schema_;
    std::map<std::string, std::pair<std::shared_ptr<Key>, std::shared_ptr<Record>>> data_;

    friend class SkipListMemTable;
};

// Implementation of Freeze method (needs to be after ImmutableSkipListMemTable definition)
std::unique_ptr<ImmutableMemTable> SkipListMemTable::Freeze() {
    std::unique_lock<std::mutex> lock(mutex_);

    // Create immutable copy
    auto immutable = std::make_unique<ImmutableSkipListMemTable>(schema_);

    // Move data to immutable memtable
    immutable->AddData(std::move(data_));

    // Clear current memtable
    data_.clear();
    current_size_ = 0;

    return immutable;
}

// Simple key implementation for benchmarks
class BenchKey : public marble::Key {
public:
    explicit BenchKey(int64_t id) : id_(id) {}
    explicit BenchKey(const std::string& str) : id_(std::stoll(str)) {}

    int Compare(const marble::Key& other) const override {
        const auto& other_key = static_cast<const BenchKey&>(other);
        if (id_ < other_key.id_) return -1;
        if (id_ > other_key.id_) return 1;
        return 0;
    }

    std::shared_ptr<marble::Key> Clone() const override {
        return std::make_shared<BenchKey>(id_);
    }

    std::string ToString() const override {
        return std::to_string(id_);
    }

    size_t Hash() const override {
        return std::hash<int64_t>()(id_);
    }

    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return arrow::MakeScalar(id_);
    }

private:
    int64_t id_;
};

// Simple record implementation for benchmarks
struct BenchRecord {
    int64_t id;
    std::string name;
    std::string value;
    int32_t category;
};

class BenchRecordImpl : public marble::Record {
public:
    explicit BenchRecordImpl(BenchRecord record) : record_(std::move(record)) {}

    std::shared_ptr<marble::Key> GetKey() const override {
        return std::make_shared<BenchKey>(record_.id);
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::utf8()),
            arrow::field("category", arrow::int32())
        });
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override {
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::StringBuilder value_builder;
        arrow::Int32Builder category_builder;

        id_builder.Append(record_.id).ok();
        name_builder.Append(record_.name).ok();
        value_builder.Append(record_.value).ok();
        category_builder.Append(record_.category).ok();

        std::shared_ptr<arrow::Array> id_array, name_array, value_array, category_array;
        id_builder.Finish(&id_array).ok();
        name_builder.Finish(&name_array).ok();
        value_builder.Finish(&value_array).ok();
        category_builder.Finish(&category_array).ok();

        return arrow::RecordBatch::Make(GetArrowSchema(), 1,
            {id_array, name_array, value_array, category_array});
    }

    size_t Size() const {
        return sizeof(BenchRecord) + record_.name.size() + record_.value.size();
    }

    std::unique_ptr<RecordRef> AsRecordRef() const override {
        // Return a simple implementation - in practice this would provide
        // zero-copy access to the underlying data
        return nullptr; // Placeholder
    }

private:
    BenchRecord record_;
};

// Iterator for ArrowSSTable with predicate filtering
class ArrowSSTableIterator : public MemTable::Iterator {
public:
    ArrowSSTableIterator(std::shared_ptr<arrow::RecordBatch> batch)
        : batch_(batch), current_row_(0), predicates_() {}

    ArrowSSTableIterator(std::shared_ptr<arrow::RecordBatch> batch,
                        const std::vector<ColumnPredicate>& predicates)
        : batch_(batch), current_row_(0), predicates_(predicates) {}

    ~ArrowSSTableIterator() override = default;

    bool Valid() const override {
        return batch_ && current_row_ < static_cast<size_t>(batch_->num_rows());
    }

    void SeekToFirst() override {
        current_row_ = 0;
        // Skip to first matching row if predicates exist
        while (Valid() && !MatchesPredicates()) {
            ++current_row_;
        }
    }

    void SeekToLast() override {
        current_row_ = batch_->num_rows() - 1;
        // Skip to last matching row if predicates exist
        while (current_row_ > 0 && !MatchesPredicates()) {
            --current_row_;
        }
    }

    void Seek(const Key& target) override {
        // Simplified seek - would need proper key comparison
        current_row_ = 0;  // Start from beginning
        while (Valid() && !MatchesTarget(target)) {
            ++current_row_;
        }
    }

    void Next() override {
        if (Valid()) {
            ++current_row_;
            // Skip rows that don't match predicates
            while (Valid() && !MatchesPredicates()) {
                ++current_row_;
            }
        }
    }

    void Prev() override {
        if (current_row_ > 0) {
            --current_row_;
            // Skip rows that don't match predicates
            while (current_row_ > 0 && !MatchesPredicates()) {
                --current_row_;
            }
        }
    }

    std::shared_ptr<marble::Key> GetKey() const override {
        if (!Valid()) return nullptr;
        // Simplified - return a key based on row index
        return std::make_shared<BenchKey>(static_cast<int64_t>(current_row_));
    }

    std::shared_ptr<Record> Value() const override {
        if (!Valid()) return nullptr;

        // Create a record from the current row
        // This is simplified - real implementation would extract proper data
        BenchRecord record{static_cast<int64_t>(current_row_),
                          "record_" + std::to_string(current_row_),
                          "value_" + std::to_string(current_row_),
                          static_cast<int32_t>(current_row_ % 100)};
        return std::make_shared<BenchRecordImpl>(record);
    }

    marble::Status GetStatus() const override {
        return marble::Status::OK();
    }

    std::unique_ptr<RecordRef> ValueRef() const override {
        // Return a simple implementation that wraps the record
        // In a full implementation, this would provide zero-copy access
        return nullptr; // Placeholder
    }

private:
    bool MatchesTarget(const Key& target) const {
        // Simplified target matching
        return true;
    }

    bool MatchesPredicates() const {
        if (predicates_.empty()) return true;

        // Apply predicates to current row
        for (const auto& pred : predicates_) {
            if (!EvaluatePredicate(pred)) {
                return false;
            }
        }
        return true;
    }

    bool EvaluatePredicate(const ColumnPredicate& pred) const {
        if (!Valid()) return false;

        // Simplified predicate evaluation - would need proper type checking
        // For now, just return true for demonstration
        return true;
    }

    std::shared_ptr<arrow::RecordBatch> batch_;
    size_t current_row_;
    std::vector<ColumnPredicate> predicates_;
};

// SSTable implementation using Arrow and filesystem
class ArrowSSTable : public SSTable {
public:
    explicit ArrowSSTable(const std::string& filename)
        : SSTable()
        , filename_(filename) {}

    static Status Open(const std::string& filename, std::unique_ptr<SSTable>* table) {
        auto sstable = std::make_unique<ArrowSSTable>(filename);

        // Load metadata and data
        auto status = sstable->LoadMetadata();
        if (!status.ok()) return status;

        *table = std::move(sstable);
        return Status::OK();
    }

    static Status Create(const std::string& filename,
                        const std::shared_ptr<arrow::RecordBatch>& batch,
                        std::unique_ptr<SSTable>* table) {
        auto sstable = std::make_unique<ArrowSSTable>(filename);

        // Write the batch to file
        auto status = sstable->WriteBatch(batch);
        if (!status.ok()) return status;

        // Load metadata
        status = sstable->LoadMetadata();
        if (!status.ok()) return status;

        *table = std::move(sstable);
        return Status::OK();
    }

    const Metadata& GetMetadata() const override {
        return metadata_;
    }

    Status Get(const Key& key, std::shared_ptr<Record>* record) const override {
        if (!batch_) {
            return Status::NotFound("SSTable not loaded");
        }

        // This is a simplified implementation - real implementation would need
        // indexing or binary search through the sorted data
        for (int64_t i = 0; i < batch_->num_rows(); ++i) {
            // Simplified key matching - would need proper key comparison
            *record = nullptr;  // Placeholder
            return Status::NotFound("Key lookup not implemented");
        }

        return Status::NotFound("Key not found");
    }

    std::unique_ptr<MemTable::Iterator> NewIterator() override {
        // Return an iterator over the SSTable data
        return std::make_unique<ArrowSSTableIterator>(batch_);
    }

    std::unique_ptr<MemTable::Iterator> NewIterator(const std::vector<ColumnPredicate>& predicates) override {
        // Return an iterator with predicate filtering
        return std::make_unique<ArrowSSTableIterator>(batch_, predicates);
    }

    bool KeyMayMatch(const Key& key) const override {
        // Check if key is within the range of this SSTable
        return true;  // Placeholder - would use bloom filter or range check
    }

    Status GetBloomFilter(std::string* bloom_filter) const override {
        *bloom_filter = "";  // Placeholder
        return Status::NotImplemented("Bloom filters not implemented");
    }

    Status ReadRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch) const override {
        if (!batch_) {
            return Status::NotFound("SSTable not loaded");
        }

        *batch = batch_;
        return Status::OK();
    }

private:
    Status LoadMetadata() {
        // Load the arrow file and extract metadata
        auto fs = FileSystem::CreateLocal();
        std::unique_ptr<FileHandle> file_handle;
        auto status = fs->OpenFile(filename_, FileOpenFlags::kRead, &file_handle);

        if (!status.ok()) {
            return status;
        }

        // Read file size
        size_t file_size;
        auto get_size_status = file_handle->GetSize(&file_size);
        if (!get_size_status.ok()) return get_size_status;
        metadata_.file_size = file_size;
        metadata_.filename = filename_;

        // This is simplified - real implementation would read the Arrow file
        // and extract min/max keys and other metadata
        metadata_.num_entries = 0;  // Would be calculated from file

        return Status::OK();
    }

    Status WriteBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
        // Write the batch to file using Arrow IPC format
        auto fs = FileSystem::CreateLocal();
        std::unique_ptr<FileHandle> file_handle;
        auto status = fs->OpenFile(filename_, static_cast<FileOpenFlags>(
            static_cast<int>(FileOpenFlags::kWrite) | static_cast<int>(FileOpenFlags::kCreate)),
            &file_handle);

        if (!status.ok()) {
            return status;
        }

        // This is simplified - real implementation would use Arrow IPC writer
        batch_ = batch;

        return Status::OK();
    }

    std::string filename_;
    Metadata metadata_;
    std::shared_ptr<arrow::RecordBatch> batch_;
};

// Factory functions
std::unique_ptr<MemTable> CreateSkipListMemTable(std::shared_ptr<Schema> schema) {
    return std::make_unique<SkipListMemTable>(std::move(schema));
}

} // namespace marble
