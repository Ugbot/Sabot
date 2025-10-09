#include <marble/marble.h>
#include <iostream>
#include <memory>
#include <vector>

using namespace marble;

// Simple key implementation for the example
class SimpleKey : public Key {
public:
    explicit SimpleKey(int64_t id) : id_(id) {}
    explicit SimpleKey(const std::string& str) : id_(std::stoll(str)) {}

    int Compare(const Key& other) const override {
        const auto& other_key = static_cast<const SimpleKey&>(other);
        if (id_ < other_key.id_) return -1;
        if (id_ > other_key.id_) return 1;
        return 0;
    }

    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<SimpleKey>(id_);
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

// Simple record implementation for the example
struct SimpleRecord {
    int64_t id;
    std::string name;
    std::string value;
    int32_t category;
};

class SimpleRecordImpl : public Record {
public:
    explicit SimpleRecordImpl(SimpleRecord record) : record_(std::move(record)) {}

    std::shared_ptr<Key> GetKey() const override {
        return std::make_shared<SimpleKey>(record_.id);
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

        ARROW_RETURN_NOT_OK(id_builder.Append(record_.id));
        ARROW_RETURN_NOT_OK(name_builder.Append(record_.name));
        ARROW_RETURN_NOT_OK(value_builder.Append(record_.value));
        ARROW_RETURN_NOT_OK(category_builder.Append(record_.category));

        std::shared_ptr<arrow::Array> id_array, name_array, value_array, category_array;
        ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
        ARROW_RETURN_NOT_OK(name_builder.Finish(&name_array));
        ARROW_RETURN_NOT_OK(value_builder.Finish(&value_array));
        ARROW_RETURN_NOT_OK(category_builder.Finish(&category_array));

        return arrow::RecordBatch::Make(GetArrowSchema(), 1,
            {id_array, name_array, value_array, category_array});
    }

    size_t Size() const {
        return sizeof(SimpleRecord) + record_.name.size() + record_.value.size();
    }

    std::unique_ptr<RecordRef> AsRecordRef() const override {
        // Return a simple implementation that wraps the record
        // In a full implementation, this would provide zero-copy access
        return nullptr; // Placeholder
    }

    // Getters
    int64_t id() const { return record_.id; }
    const std::string& name() const { return record_.name; }
    const std::string& value() const { return record_.value; }
    int32_t category() const { return record_.category; }

private:
    SimpleRecord record_;
};

int main() {
    std::cout << "MarbleDB ClickHouse-Style Indexing Demo" << std::endl;
    std::cout << "=======================================" << std::endl;

    std::cout << "\nClickHouse-style indexing provides:" << std::endl;
    std::cout << "• Sparse indexes: Index every Nth key instead of all keys" << std::endl;
    std::cout << "• Block-level statistics: Min/max values per data block" << std::endl;
    std::cout << "• Bloom filters: Fast key existence checks" << std::endl;
    std::cout << "• Efficient range queries and point lookups" << std::endl;

    // Create a sample record batch for demonstration
    std::vector<std::shared_ptr<SimpleRecordImpl>> records;
    for (int i = 0; i < 10000; ++i) {
        SimpleRecord record{static_cast<int64_t>(i),
                           "user_" + std::to_string(i),
                           "data_" + std::to_string(i),
                           static_cast<int32_t>(i % 100)};
        records.push_back(std::make_shared<SimpleRecordImpl>(record));
    }

    // Build Arrow RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::StringBuilder value_builder;
    arrow::Int32Builder category_builder;

    for (const auto& record : records) {
        id_builder.Append(record->id());
        name_builder.Append(record->name());
        value_builder.Append(record->value());
        category_builder.Append(record->category());
    }

    std::shared_ptr<arrow::Array> id_array, name_array, value_array, category_array;
    id_builder.Finish(&id_array);
    name_builder.Finish(&name_array);
    value_builder.Finish(&value_array);
    category_builder.Finish(&category_array);

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
        arrow::field("category", arrow::int32())
    });

    auto record_batch = arrow::RecordBatch::Make(schema, records.size(),
        {id_array, name_array, value_array, category_array});

    std::cout << "\nCreated RecordBatch with " << record_batch->num_rows() << " rows" << std::endl;

    // Demonstrate indexing options
    DBOptions options;
    options.enable_sparse_index = true;
    options.index_granularity = 1000;  // Index every 1000 rows
    options.target_block_size = 1000;  // 1000 rows per block
    options.enable_bloom_filter = true;
    options.bloom_filter_bits_per_key = 10;

    std::cout << "\nIndexing Configuration:" << std::endl;
    std::cout << "• Sparse index enabled: " << (options.enable_sparse_index ? "Yes" : "No") << std::endl;
    std::cout << "• Index granularity: " << options.index_granularity << " rows" << std::endl;
    std::cout << "• Target block size: " << options.target_block_size << " rows" << std::endl;
    std::cout << "• Bloom filter enabled: " << (options.enable_bloom_filter ? "Yes" : "No") << std::endl;
    std::cout << "• Bloom filter bits per key: " << options.bloom_filter_bits_per_key << std::endl;

    // Create SSTable with ClickHouse-style indexing
    std::unique_ptr<SSTable> sstable;
    auto status = CreateSSTable("/tmp/test_sstable.sst", record_batch, options, &sstable);

    if (!status.ok()) {
        std::cerr << "Failed to create SSTable: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "\nSSTable created successfully!" << std::endl;

    // Examine the indexing metadata
    const auto& metadata = sstable->GetMetadata();
    std::cout << "\nSSTable Metadata:" << std::endl;
    std::cout << "• Total entries: " << metadata.num_entries << std::endl;
    std::cout << "• Block size: " << metadata.block_size << std::endl;
    std::cout << "• Index granularity: " << metadata.index_granularity << std::endl;
    std::cout << "• Has sparse index: " << (metadata.has_sparse_index ? "Yes" : "No") << std::endl;
    std::cout << "• Has bloom filter: " << (metadata.has_bloom_filter ? "Yes" : "No") << std::endl;
    std::cout << "• Number of blocks: " << metadata.block_stats.size() << std::endl;
    std::cout << "• Sparse index entries: " << metadata.sparse_index.size() << std::endl;

    // Demonstrate block-level statistics
    std::cout << "\nBlock Statistics (first 5 blocks):" << std::endl;
    size_t blocks_to_show = std::min(size_t(5), metadata.block_stats.size());
    for (size_t i = 0; i < blocks_to_show; ++i) {
        const auto& block = metadata.block_stats[i];
        std::cout << "  Block " << i << ": "
                  << "rows=" << block.row_count
                  << ", min_key=" << (block.min_key ? block.min_key->ToString() : "null")
                  << ", max_key=" << (block.max_key ? block.max_key->ToString() : "null")
                  << std::endl;
    }

    // Demonstrate sparse index
    if (!metadata.sparse_index.empty()) {
        std::cout << "\nSparse Index (first 5 entries):" << std::endl;
        size_t entries_to_show = std::min(size_t(5), metadata.sparse_index.size());
        for (size_t i = 0; i < entries_to_show; ++i) {
            const auto& entry = metadata.sparse_index[i];
            std::cout << "  Index " << i << ": "
                      << "key=" << entry.key->ToString()
                      << ", block=" << entry.block_index
                      << ", row=" << entry.row_index
                      << std::endl;
        }
    }

    // Demonstrate key lookups with bloom filter
    std::cout << "\nKey Lookup Tests:" << std::endl;

    // Test existing keys
    for (int test_key : {100, 1000, 5000, 9999}) {
        SimpleKey key(test_key);
        bool may_contain = sstable->KeyMayMatch(key);
        std::cout << "  Key " << test_key << " may exist: " << (may_contain ? "Yes" : "No");

        // Try to get the actual record
        std::shared_ptr<Record> record;
        status = sstable->Get(key, &record);
        std::cout << ", found: " << (status.ok() ? "Yes" : "No") << std::endl;
    }

    // Test non-existing keys
    for (int test_key : {10000, 15000, -1}) {
        SimpleKey key(test_key);
        bool may_contain = sstable->KeyMayMatch(key);
        std::cout << "  Key " << test_key << " may exist: " << (may_contain ? "Yes" : "No") << std::endl;
    }

    // Demonstrate bloom filter serialization
    std::string bloom_data;
    status = sstable->GetBloomFilter(&bloom_data);
    if (status.ok()) {
        std::cout << "\nBloom filter size: " << bloom_data.size() << " bytes" << std::endl;
    }

    std::cout << "\n=== ClickHouse-Style Indexing Benefits ===" << std::endl;
    std::cout << "✓ Reduced index size: Only index every Nth key" << std::endl;
    std::cout << "✓ Fast key existence checks: Bloom filter" << std::endl;
    std::cout << "✓ Block-level pruning: Skip irrelevant blocks" << std::endl;
    std::cout << "✓ Efficient range queries: Use sparse index for navigation" << std::endl;
    std::cout << "✓ Memory efficient: Index fits in CPU cache" << std::endl;
    std::cout << "✓ Scalable: Works well with large datasets" << std::endl;

    std::cout << "\nClickHouse-style indexing demonstration completed!" << std::endl;
    std::cout << "This provides MarbleDB with advanced indexing capabilities" << std::endl;
    std::cout << "similar to ClickHouse's MergeTree engine." << std::endl;

    return 0;
}
