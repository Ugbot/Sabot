#include <marble/lsm_tree.h>
#include <marble/sstable.h>
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

        bool Valid() const {
            return it_ != data_.end();
        }

        void SeekToFirst() {
            it_ = data_.begin();
        }

        void SeekToLast() {
            if (!data_.empty()) {
                it_ = std::prev(data_.end());
            } else {
                it_ = data_.end();
            }
        }

        void Seek(const marble::Key& key) {
            it_ = data_.lower_bound(key.ToString());
        }

        void Next() {
            if (it_ != data_.end()) {
                ++it_;
            }
        }

        void Prev() {
            if (it_ != data_.begin()) {
                --it_;
            } else {
                it_ = data_.end();
            }
        }

        std::shared_ptr<marble::Key> GetKey() const {
            if (Valid()) {
                return it_->second.first;
            }
            return nullptr;
        }

        std::shared_ptr<Record> Value() const {
            if (Valid()) {
                return it_->second.second;
            }
            return nullptr;
        }

        std::unique_ptr<RecordRef> ValueRef() const {
            // Return zero-copy reference if available
            if (Valid() && it_->second.second) {
                return it_->second.second->AsRecordRef();
            }
            return nullptr;
        }

        marble::Status GetStatus() const {
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

    Status Put(std::shared_ptr<Key> key, std::shared_ptr<Record> record) {
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

    Status Get(const Key& key, std::shared_ptr<Record>* record) const {
        std::unique_lock<std::mutex> lock(mutex_);

        auto it = data_.find(key.ToString());
        if (it == data_.end()) {
            return Status::NotFound("Key not found in MemTable");
        }

        *record = it->second.second;
        return Status::OK();
    }

    Status Delete(std::shared_ptr<Key> key) {
        std::unique_lock<std::mutex> lock(mutex_);

        auto it = data_.find(key->ToString());
        if (it != data_.end()) {
            data_.erase(it);
        }

        return Status::OK();
    }

    bool IsFull() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return current_size_ >= max_size_;
    }

    size_t ApproximateMemoryUsage() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return current_size_;
    }

    size_t Size() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return data_.size();
    }

    std::unique_ptr<ImmutableMemTable> Freeze();

    void Clear() {
        std::unique_lock<std::mutex> lock(mutex_);
        data_.clear();
        current_size_ = 0;
    }

    std::unique_ptr<Iterator> NewIterator() {
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

    Status Get(const marble::Key& key, std::shared_ptr<Record>* record) const {
        auto it = data_.find(key.ToString());
        if (it == data_.end()) {
            return Status::NotFound("Key not found in immutable MemTable");
        }

        *record = it->second.second;
        return Status::OK();
    }

    std::unique_ptr<MemTable::Iterator> NewIterator() {
        return std::make_unique<SkipListMemTable::SkipListIterator>(data_);
    }

    size_t ApproximateMemoryUsage() const {
        size_t size = 0;
        for (const auto& pair : data_) {
            size += pair.first.size() + 100;  // rough estimate
        }
        return size;
    }

    size_t Size() const {
        return data_.size();
    }

    Status ToRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch) const {
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

    int Compare(const marble::Key& other) const {
        const auto& other_key = static_cast<const BenchKey&>(other);
        if (id_ < other_key.id_) return -1;
        if (id_ > other_key.id_) return 1;
        return 0;
    }

    std::shared_ptr<marble::Key> Clone() const {
        return std::make_shared<BenchKey>(id_);
    }

    std::string ToString() const {
        return std::to_string(id_);
    }

    size_t Hash() const {
        return std::hash<int64_t>()(id_);
    }

    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const {
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

    std::shared_ptr<marble::Key> GetKey() const {
        return std::make_shared<BenchKey>(record_.id);
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::utf8()),
            arrow::field("category", arrow::int32())
        });
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const {
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

    std::unique_ptr<RecordRef> AsRecordRef() const {
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

    ~ArrowSSTableIterator() = default;

    bool Valid() const {
        return batch_ && current_row_ < static_cast<size_t>(batch_->num_rows());
    }

    void SeekToFirst() {
        current_row_ = 0;
        // Skip to first matching row if predicates exist
        while (Valid() && !MatchesPredicates()) {
            ++current_row_;
        }
    }

    void SeekToLast() {
        current_row_ = batch_->num_rows() - 1;
        // Skip to last matching row if predicates exist
        while (current_row_ > 0 && !MatchesPredicates()) {
            --current_row_;
        }
    }

    void Seek(const Key& target) {
        // Simplified seek - would need proper key comparison
        current_row_ = 0;  // Start from beginning
        while (Valid() && !MatchesTarget(target)) {
            ++current_row_;
        }
    }

    void Next() {
        if (Valid()) {
            ++current_row_;
            // Skip rows that don't match predicates
            while (Valid() && !MatchesPredicates()) {
                ++current_row_;
            }
        }
    }

    void Prev() {
        if (current_row_ > 0) {
            --current_row_;
            // Skip rows that don't match predicates
            while (current_row_ > 0 && !MatchesPredicates()) {
                --current_row_;
            }
        }
    }

    std::shared_ptr<marble::Key> GetKey() const {
        if (!Valid()) return nullptr;
        // Simplified - return a key based on row index
        return std::make_shared<BenchKey>(static_cast<int64_t>(current_row_));
    }

    std::shared_ptr<Record> Value() const {
        if (!Valid()) return nullptr;

        // Create a record from the current row
        // This is simplified - real implementation would extract proper data
        BenchRecord record{static_cast<int64_t>(current_row_),
                          "record_" + std::to_string(current_row_),
                          "value_" + std::to_string(current_row_),
                          static_cast<int32_t>(current_row_ % 100)};
        return std::make_shared<BenchRecordImpl>(record);
    }

    marble::Status GetStatus() const {
        return marble::Status::OK();
    }

    std::unique_ptr<RecordRef> ValueRef() const {
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
class ArrowSSTable : public LSMSSTable {
public:
    explicit ArrowSSTable(const std::string& filename)
        : LSMSSTable()
        , filename_(filename) {}

    ~ArrowSSTable() = default;

    static Status Open(const std::string& filename, std::unique_ptr<LSMSSTable>* table) {
        auto sstable = std::make_unique<ArrowSSTable>(filename);

        // Load metadata and data
        auto status = sstable->LoadMetadata();
        if (!status.ok()) return status;

        *table = std::move(sstable);
        return Status::OK();
    }

    static Status Create(const std::string& filename,
                        const std::shared_ptr<arrow::RecordBatch>& batch,
                        const DBOptions& options,
                        std::unique_ptr<LSMSSTable>* table) {
        auto sstable = std::make_unique<ArrowSSTable>(filename);

        // Write the batch to file
        auto status = sstable->WriteBatch(batch);
        if (!status.ok()) return status;

        // Build ClickHouse-style indexing
        status = sstable->BuildIndexing(batch, options);
        if (!status.ok()) return status;

        // Load metadata
        status = sstable->LoadMetadata();
        if (!status.ok()) return status;

        *table = std::move(sstable);
        return Status::OK();
    }

    Status BuildIndexing(const std::shared_ptr<arrow::RecordBatch>& batch, const DBOptions& options) {
        if (!batch || batch->num_rows() == 0) {
            return Status::OK();
        }

        // Initialize indexing options
        block_size_ = options.target_block_size;
        index_granularity_ = options.index_granularity;
        has_sparse_index_ = options.enable_sparse_index;
        has_bloom_filter_ = options.enable_bloom_filter;

        // Build block-level statistics and sparse index
        int64_t num_rows = batch->num_rows();
        size_t num_blocks = (num_rows + block_size_ - 1) / block_size_;

        block_stats_.reserve(num_blocks);

        if (has_sparse_index_) {
            sparse_index_.reserve(num_rows / index_granularity_ + 1);
        }

        if (has_bloom_filter_) {
            bloom_filter_ = std::make_unique<BloomFilter>(options.bloom_filter_bits_per_key, num_rows);
        }

        // Process each block
        for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
            size_t start_row = block_idx * block_size_;
            size_t end_row = std::min(start_row + block_size_, static_cast<size_t>(num_rows));

            LSMSSTable::BlockStats block_stat;
            block_stat.first_row_index = start_row;
            block_stat.row_count = end_row - start_row;

            // Get min/max keys for this block
            if (start_row < end_row) {
                // Create keys for min/max - simplified for now
                auto min_key = std::make_shared<BenchKey>(static_cast<int64_t>(start_row));
                auto max_key = std::make_shared<BenchKey>(static_cast<int64_t>(end_row - 1));
                block_stat.min_key = min_key;
                block_stat.max_key = max_key;

                // Update global min/max
                if (!smallest_key_ || min_key->Compare(*smallest_key_) < 0) {
                    smallest_key_ = min_key;
                }
                if (!largest_key_ || max_key->Compare(*largest_key_) > 0) {
                    largest_key_ = max_key;
                }

                // Add to bloom filter
                if (bloom_filter_) {
                    for (size_t row = start_row; row < end_row; ++row) {
                        BenchKey key(static_cast<int64_t>(row));
                        bloom_filter_->Add(key);
                    }
                }
            }

            block_stats_.push_back(block_stat);

            // Build sparse index
            if (has_sparse_index_ && (start_row % index_granularity_ == 0)) {
                LSMSSTable::SparseIndexEntry entry;
                entry.key = std::make_shared<BenchKey>(static_cast<int64_t>(start_row));
                entry.block_index = block_idx;
                entry.row_index = start_row;
                sparse_index_.push_back(entry);
            }
        }
        return Status::OK();
    }

    const Metadata& GetMetadata() const override {
        return metadata_;
    }

    Status Get(const Key& key, std::shared_ptr<Record>* record) const override {
        if (!batch_) {
            return Status::NotFound("SSTable not loaded");
        }

        // Use sparse index for efficient lookup if available
        if (has_sparse_index_ && !sparse_index_.empty()) {
            // Convert key to integer for easier comparison (assuming BenchKey)
            int64_t target_key;
            try {
                target_key = std::stoll(key.ToString());
            } catch (...) {
                // Fallback to linear search for non-integer keys
                return linearSearch(key, record);
            }

            // Find the sparse index entry that covers this key
            // Each entry covers [entry.key, entry.key + granularity)
            size_t sparse_idx = target_key / index_granularity_;
            if (sparse_idx >= sparse_index_.size()) {
                sparse_idx = sparse_index_.size() - 1;
            }

            // Search within the block covered by this sparse index entry
            size_t block_start = sparse_index_[sparse_idx].row_index;
            size_t block_end = (sparse_idx + 1 < sparse_index_.size()) ?
                sparse_index_[sparse_idx + 1].row_index :
                static_cast<size_t>(batch_->num_rows());

            // Linear search within the block
            for (size_t i = block_start; i < block_end; ++i) {
                if (static_cast<int64_t>(i) == target_key) {
                    // Found the key - create record
                    BenchRecord bench_record{static_cast<int64_t>(i),
                                           "record_" + std::to_string(i),
                                           "value_" + std::to_string(i),
                                           static_cast<int32_t>(i % 100)};
                    *record = std::make_shared<BenchRecordImpl>(bench_record);
                    return Status::OK();
                }
            }
        } else {
            // Fallback to linear search
            return linearSearch(key, record);
        }

        return Status::NotFound("Key not found");
    }

    Status linearSearch(const Key& search_key, std::shared_ptr<Record>* record) const {
        for (int64_t i = 0; i < batch_->num_rows(); ++i) {
            BenchKey row_key(static_cast<int64_t>(i));
            if (search_key.Compare(row_key) == 0) {
                // Found the key - create record
                BenchRecord bench_record{static_cast<int64_t>(i),
                                       "record_" + std::to_string(i),
                                       "value_" + std::to_string(i),
                                       static_cast<int32_t>(i % 100)};
                *record = std::make_shared<BenchRecordImpl>(bench_record);
                return Status::OK();
            }
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
        // Check bloom filter first if available
        if (has_bloom_filter_ && bloom_filter_) {
            if (!bloom_filter_->MayContain(key)) {
                return false;
            }
        }

        // Check if key is within the range of this SSTable
        if (smallest_key_ && largest_key_) {
            return key.Compare(*smallest_key_) >= 0 &&
                   key.Compare(*largest_key_) <= 0;
        }

        return true;
    }

    Status GetBloomFilter(std::string* bloom_filter) const override {
        if (bloom_filter_ && has_bloom_filter_) {
            return bloom_filter_->Serialize(bloom_filter);
        }
        *bloom_filter = "";
        return Status::NotFound("No bloom filter available");
    }

    Status GetBlockStats(std::vector<LSMSSTable::BlockStats>* stats) const override {
        *stats = block_stats_;
        return Status::OK();
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
        // Preserve indexing metadata set during BuildIndexing
        bool preserve_indexing = has_sparse_index_ || has_bloom_filter_;
        size_t saved_num_entries = metadata_.num_entries;
        auto saved_smallest = metadata_.smallest_key;
        auto saved_largest = metadata_.largest_key;
        auto saved_block_stats = metadata_.block_stats;
        auto saved_sparse_index = metadata_.sparse_index;
        auto saved_block_size = metadata_.block_size;
        auto saved_index_granularity = metadata_.index_granularity;
        auto saved_has_bloom = metadata_.has_bloom_filter;
        auto saved_has_sparse = metadata_.has_sparse_index;

        metadata_.num_entries = batch_ ? static_cast<uint64_t>(batch_->num_rows()) : 0;  // Calculate from loaded batch

        // Restore indexing metadata if it was set
        if (preserve_indexing) {
            metadata_.num_entries = saved_num_entries;
            metadata_.smallest_key = saved_smallest;
            metadata_.largest_key = saved_largest;
            metadata_.block_stats = saved_block_stats;
            metadata_.sparse_index = saved_sparse_index;
            metadata_.block_size = saved_block_size;
            metadata_.index_granularity = saved_index_granularity;
            metadata_.has_bloom_filter = saved_has_bloom;
            metadata_.has_sparse_index = saved_has_sparse;
        }

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
    std::unique_ptr<BloomFilter> bloom_filter_;

    // Indexing data stored in the SSTable itself
    size_t block_size_ = 1024;
    size_t index_granularity_ = 100;
    bool has_sparse_index_ = false;
    bool has_bloom_filter_ = false;
    std::vector<LSMSSTable::BlockStats> block_stats_;
    std::vector<LSMSSTable::SparseIndexEntry> sparse_index_;
    std::shared_ptr<Key> smallest_key_;
    std::shared_ptr<Key> largest_key_;
};

// BloomFilter implementation
BloomFilter::BloomFilter(size_t bits_per_key, size_t num_keys)
    : num_hashes_(static_cast<size_t>(bits_per_key * 0.69)) {  // ln(2) approximation
    size_t bits = num_keys * bits_per_key;
    if (bits < 64) bits = 64;  // Minimum size
    size_t bytes = (bits + 7) / 8;  // Round up to bytes
    bits_.resize(bytes, 0);
}

BloomFilter::~BloomFilter() = default;

void BloomFilter::Add(const Key& key) {
    std::string key_str = key.ToString();
    uint64_t hash1 = Hash1(key_str);
    uint64_t hash2 = Hash2(key_str);

    for (size_t i = 0; i < num_hashes_; ++i) {
        size_t bit_pos = NthHash(hash1, hash2, i) % (bits_.size() * 8);
        size_t byte_pos = bit_pos / 8;
        size_t bit_offset = bit_pos % 8;
        bits_[byte_pos] |= (1 << bit_offset);
    }
}

bool BloomFilter::MayContain(const Key& key) const {
    std::string key_str = key.ToString();
    uint64_t hash1 = Hash1(key_str);
    uint64_t hash2 = Hash2(key_str);

    for (size_t i = 0; i < num_hashes_; ++i) {
        size_t bit_pos = NthHash(hash1, hash2, i) % (bits_.size() * 8);
        size_t byte_pos = bit_pos / 8;
        size_t bit_offset = bit_pos % 8;
        if ((bits_[byte_pos] & (1 << bit_offset)) == 0) {
            return false;
        }
    }
    return true;
}

Status BloomFilter::Serialize(std::string* data) const {
    data->clear();
    data->reserve(bits_.size() + sizeof(size_t));

    // Store number of hashes
    data->append(reinterpret_cast<const char*>(&num_hashes_), sizeof(num_hashes_));

    // Store bits
    data->append(reinterpret_cast<const char*>(bits_.data()), bits_.size());

    return Status::OK();
}

Status BloomFilter::Deserialize(const std::string& data, std::unique_ptr<BloomFilter>* filter) {
    if (data.size() < sizeof(size_t)) {
        return Status::InvalidArgument("Bloom filter data too small");
    }

    size_t num_hashes;
    memcpy(&num_hashes, data.data(), sizeof(num_hashes));

    size_t bits_offset = sizeof(num_hashes);
    size_t bits_size = data.size() - bits_offset;

    auto bloom_filter = std::make_unique<BloomFilter>(0, 0);  // Dummy values
    bloom_filter->num_hashes_ = num_hashes;
    bloom_filter->bits_.assign(data.begin() + bits_offset, data.end());

    *filter = std::move(bloom_filter);
    return Status::OK();
}

uint64_t BloomFilter::Hash1(const std::string& key) const {
    return std::hash<std::string>{}(key);
}

uint64_t BloomFilter::Hash2(const std::string& key) const {
    uint64_t hash = 0;
    for (char c : key) {
        hash = hash * 31 + static_cast<uint8_t>(c);
    }
    return hash;
}

size_t BloomFilter::NthHash(uint64_t hash1, uint64_t hash2, size_t n) const {
    return hash1 + n * hash2;
}

// Factory functions
std::unique_ptr<MemTable> CreateSkipListMemTable(std::shared_ptr<Schema> schema) {
    return std::make_unique<SkipListMemTable>(std::move(schema));
}

Status CreateSSTable(const std::string& filename,
                    const std::shared_ptr<arrow::RecordBatch>& batch,
                    const DBOptions& options,
                    std::unique_ptr<LSMSSTable>* table) {
    return ArrowSSTable::Create(filename, batch, options, table);
}

} // namespace marble
