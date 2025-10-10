/************************************************************************
MarbleDB Query Performance Benchmark

Tests predicate pushdown, zone maps, and block skipping benefits.
Shows ClickHouse-style indexing advantages for analytical queries.
**************************************************************************/

#include <marble/marble.h>
#include <iostream>
#include <chrono>
#include <random>
#include <iomanip>

using namespace marble;

// Reuse BenchKey from other benchmarks
class BenchKey : public Key {
public:
    explicit BenchKey(int64_t id) : id_(id) {}
    int Compare(const Key& other) const override {
        const auto& other_key = static_cast<const BenchKey&>(other);
        if (id_ < other_key.id_) return -1;
        if (id_ > other_key.id_) return 1;
        return 0;
    }
    std::shared_ptr<Key> Clone() const override { return std::make_shared<BenchKey>(id_); }
    std::string ToString() const override { return std::to_string(id_); }
    size_t Hash() const override { return std::hash<int64_t>()(id_); }
    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return arrow::MakeScalar(id_);
    }
private:
    int64_t id_;
};

// Test: Bloom filter effectiveness
void BenchmarkBloomFilterLookups(const std::unique_ptr<LSMSSTable>& sstable, size_t num_lookups) {
    std::cout << "\n=== Benchmark: Bloom Filter Point Lookups ===" << std::endl;
    std::cout << "Testing " << num_lookups << " lookups (50% exist, 50% don't)" << std::endl;
    
    const auto& metadata = sstable->GetMetadata();
    size_t total_rows = metadata.num_entries;
    
    std::mt19937_64 gen(42);
    std::uniform_int_distribution<int64_t> dist(0, total_rows * 2); // Half won't exist
    
    auto start = std::chrono::high_resolution_clock::now();
    
    size_t bloom_hits = 0;
    size_t actual_found = 0;
    
    for (size_t i = 0; i < num_lookups; ++i) {
        int64_t lookup_key = dist(gen);
        BenchKey key(lookup_key);
        
        // Check bloom filter first
        bool may_exist = sstable->KeyMayMatch(key);
        if (may_exist) {
            bloom_hits++;
            
            // Actually try to find it
            std::shared_ptr<Record> record;
            auto status = sstable->Get(key, &record);
            if (status.ok()) {
                actual_found++;
            }
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    double duration_ms = duration.count() / 1000.0;
    double throughput = num_lookups / (duration_ms / 1000.0);
    double false_positive_rate = bloom_hits > actual_found ? 
        (static_cast<double>(bloom_hits - actual_found) / bloom_hits) : 0.0;
    
    std::cout << "Lookups performed: " << num_lookups << std::endl;
    std::cout << "Bloom filter hits: " << bloom_hits << std::endl;
    std::cout << "Actually found: " << actual_found << std::endl;
    std::cout << "False positive rate: " << std::fixed << std::setprecision(2) << (false_positive_rate * 100) << "%" << std::endl;
    std::cout << "Duration: " << duration_ms << " ms" << std::endl;
    std::cout << "Throughput: " << throughput << " lookups/sec" << std::endl;
    std::cout << "Avg latency: " << (duration.count() / static_cast<double>(num_lookups)) << " μs/lookup" << std::endl;
    
}

// Test: Block skipping with predicates
void BenchmarkBlockSkipping(const std::unique_ptr<LSMSSTable>& sstable, size_t num_rows) {
    std::cout << "\n=== Benchmark: Block-Level Skipping (Zone Maps) ===" << std::endl;
    
    const auto& metadata = sstable->GetMetadata();
    
    if (!metadata.has_sparse_index || metadata.block_stats.empty()) {
        std::cout << "No block statistics available - skipping test" << std::endl;
        return;
    }
    
    std::cout << "Analyzing " << metadata.block_stats.size() << " blocks for skipping potential" << std::endl;
    
    // Test query: WHERE id > (num_rows * 0.9)
    // Should skip 90% of blocks
    size_t filter_threshold = num_rows * 9 / 10;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    size_t blocks_scanned = 0;
    size_t blocks_skipped = 0;
    size_t matching_rows = 0;
    
    for (const auto& block_stat : metadata.block_stats) {
        // Check if block max_key < filter threshold
        if (block_stat.max_key) {
            BenchKey threshold_key(filter_threshold);
            if (block_stat.max_key->Compare(threshold_key) < 0) {
                // Entire block is below threshold - skip it!
                blocks_skipped++;
                continue;
            }
        }
        
        // Scan this block
        blocks_scanned++;
        matching_rows += block_stat.row_count; // Simplified - assume all rows match
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    double skip_ratio = static_cast<double>(blocks_skipped) / metadata.block_stats.size();
    
    std::cout << "Query: id > " << filter_threshold << std::endl;
    std::cout << "Total blocks: " << metadata.block_stats.size() << std::endl;
    std::cout << "Blocks scanned: " << blocks_scanned << std::endl;
    std::cout << "Blocks skipped: " << blocks_skipped << std::endl;
    std::cout << "Skip ratio: " << std::fixed << std::setprecision(1) << (skip_ratio * 100) << "%" << std::endl;
    std::cout << "Block eval time: " << duration.count() << " μs" << std::endl;
    std::cout << "Time per block: " << (duration.count() / static_cast<double>(metadata.block_stats.size())) << " μs" << std::endl;
    
}

int main(int argc, char* argv[]) {
    std::cout << "========================================" << std::endl;
    std::cout << "  MarbleDB Query Performance Benchmark" << std::endl;
    std::cout << "========================================" << std::endl;
    
    size_t num_rows = 1000000;
    size_t num_lookups = 10000;
    
    // Parse arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--rows" && i + 1 < argc) {
            num_rows = std::stoull(argv[++i]);
        } else if (arg == "--lookups" && i + 1 < argc) {
            num_lookups = std::stoull(argv[++i]);
        }
    }
    
    std::cout << "\nGenerating test SSTable..." << std::endl;
    
    // Generate test data
    std::vector<BenchRecord> records;
    records.reserve(num_rows);
    
    int64_t base_timestamp = 1700000000000;
    std::mt19937_64 gen(42);
    std::uniform_int_distribution<int64_t> user_dist(1, 100000);
    std::uniform_real_distribution<double> amount_dist(1.0, 10000.0);
    std::vector<std::string> categories = {"groceries", "entertainment", "bills", "transfer", "other"};
    
    for (size_t i = 0; i < num_rows; ++i) {
        BenchRecord record;
        record.id = i;
        record.timestamp = base_timestamp + (i * 1000);
        record.user_id = user_dist(gen);
        record.amount = amount_dist(gen);
        record.category = categories[i % categories.size()];
        records.push_back(record);
    }
    
    // Create Arrow RecordBatch
    arrow::Int64Builder id_builder, ts_builder, user_builder;
    arrow::DoubleBuilder amount_builder;
    arrow::StringBuilder category_builder;
    
    for (const auto& record : records) {
        id_builder.Append(record.id);
        ts_builder.Append(record.timestamp);
        user_builder.Append(record.user_id);
        amount_builder.Append(record.amount);
        category_builder.Append(record.category);
    }
    
    std::shared_ptr<arrow::Array> id_array, ts_array, user_array, amount_array, category_array;
    id_builder.Finish(&id_array);
    ts_builder.Finish(&ts_array);
    user_builder.Finish(&user_array);
    amount_builder.Finish(&amount_array);
    category_builder.Finish(&category_array);
    
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("user_id", arrow::int64()),
        arrow::field("amount", arrow::float64()),
        arrow::field("category", arrow::utf8())
    });
    
    auto batch = arrow::RecordBatch::Make(schema, records.size(),
        {id_array, ts_array, user_array, amount_array, category_array});
    
    // Create SSTable with ClickHouse indexing
    DBOptions options;
    options.enable_sparse_index = true;
    options.index_granularity = 8192;
    options.target_block_size = 8192;
    options.enable_bloom_filter = true;
    options.bloom_filter_bits_per_key = 10;
    
    std::unique_ptr<LSMSSTable> sstable;
    auto status = CreateSSTable("/tmp/marble_query_bench.sst", batch, options, &sstable);
    
    if (!status.ok()) {
        std::cerr << "Failed to create SSTable: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "SSTable created with " << num_rows << " rows" << std::endl;
    
    // Run query benchmarks
    BenchmarkBloomFilterLookups(sstable, num_lookups);
    BenchmarkBlockSkipping(sstable, num_rows);
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "  Query Benchmarks Complete" << std::endl;
    std::cout << "========================================" << std::endl;
    
    // Cleanup
    std::remove("/tmp/marble_query_bench.sst");
    
    return 0;
}

