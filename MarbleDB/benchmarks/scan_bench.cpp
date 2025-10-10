/************************************************************************
MarbleDB Scan Performance Benchmark

Tests full table and range scan performance with ClickHouse sparse indexing.
Demonstrates block skipping and granule-based optimization benefits.
**************************************************************************/

#include <marble/marble.h>
#include <iostream>
#include <chrono>
#include <random>
#include <iomanip>

using namespace marble;

// Use same BenchKey and record types as write_bench
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

// Benchmark configuration
struct ScanBenchConfig {
    std::string sstable_path = "/tmp/marble_scan_bench.sst";
    size_t num_rows = 1000000;
    size_t index_granularity = 8192;
    bool enable_sparse_index = true;
    bool reverse_order = false;
};

// Benchmark: Full table scan
void BenchmarkFullScan(const std::unique_ptr<LSMSSTable>& sstable, const ScanBenchConfig& config) {
    std::cout << "\n=== Benchmark: Full Table Scan ===" << std::endl;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Create iterator
    auto iterator = sstable->NewIterator();
    
    size_t rows_scanned = 0;
    iterator->SeekToFirst();
    
    while (iterator->Valid()) {
        // Access key and value (zero-copy when possible)
        auto key = iterator->GetKey();
        auto value_ref = iterator->ValueRef();
        
        rows_scanned++;
        iterator->Next();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    double duration_sec = duration.count() / 1000.0;
    double throughput = rows_scanned / duration_sec;
    
    std::cout << "Rows scanned: " << rows_scanned << std::endl;
    std::cout << "Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput << " rows/sec" << std::endl;
    std::cout << "Throughput: " << (throughput / 1000000.0) << " M rows/sec" << std::endl;
    
}

// Benchmark: Range scan with sparse index
void BenchmarkRangeScan(const std::unique_ptr<LSMSSTable>& sstable, const ScanBenchConfig& config) {
    std::cout << "\n=== Benchmark: Range Scan (10% of data) ===" << std::endl;
    
    // Scan 10% of data (middle section)
    size_t start_key = config.num_rows * 45 / 100;
    size_t end_key = config.num_rows * 55 / 100;
    size_t expected_rows = end_key - start_key;
    
    std::cout << "Scanning keys [" << start_key << ", " << end_key << "] (~" << expected_rows << " rows)" << std::endl;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    auto iterator = sstable->NewIterator();
    BenchKey start_key_obj(start_key);
    iterator->Seek(start_key_obj);
    
    size_t rows_scanned = 0;
    while (iterator->Valid()) {
        auto key = iterator->GetKey();
        
        // Check if beyond range
        BenchKey end_key_obj(end_key);
        if (key->Compare(end_key_obj) >= 0) {
            break;
        }
        
        rows_scanned++;
        iterator->Next();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    double duration_ms = duration.count() / 1000.0;
    double throughput = rows_scanned / (duration_ms / 1000.0);
    
    std::cout << "Rows scanned: " << rows_scanned << std::endl;
    std::cout << "Duration: " << std::fixed << std::setprecision(3) << duration_ms << " ms" << std::endl;
    std::cout << "Throughput: " << throughput << " rows/sec" << std::endl;
    
    // Calculate sparse index benefit
    const auto& metadata = sstable->GetMetadata();
    if (metadata.has_sparse_index) {
        size_t total_blocks = metadata.block_stats.size();
        size_t blocks_in_range = (expected_rows + metadata.block_size - 1) / metadata.block_size;
        double skip_ratio = 1.0 - (static_cast<double>(blocks_in_range) / total_blocks);
        
        std::cout << "Sparse index stats:" << std::endl;
        std::cout << "  Total blocks: " << total_blocks << std::endl;
        std::cout << "  Blocks in range: " << blocks_in_range << std::endl;
        std::cout << "  Blocks skipped: " << (total_blocks - blocks_in_range) << std::endl;
        std::cout << "  Skip ratio: " << std::fixed << std::setprecision(1) << (skip_ratio * 100) << "%" << std::endl;
    }
    
}

// Benchmark: Reverse scan
void BenchmarkReverseScan(const std::unique_ptr<LSMSSTable>& sstable, const ScanBenchConfig& config) {
    std::cout << "\n=== Benchmark: Reverse Scan ===" << std::endl;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    auto iterator = sstable->NewIterator();
    iterator->SeekToLast();
    
    size_t rows_scanned = 0;
    while (iterator->Valid()) {
        auto key = iterator->GetKey();
        rows_scanned++;
        iterator->Prev();
        
        // Scan 10% for timing
        if (rows_scanned >= config.num_rows / 10) {
            break;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    double throughput = rows_scanned / (duration.count() / 1000.0);
    
    std::cout << "Rows scanned (reverse): " << rows_scanned << std::endl;
    std::cout << "Duration: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput << " rows/sec" << std::endl;
    
}

int main(int argc, char* argv[]) {
    std::cout << "========================================" << std::endl;
    std::cout << "  MarbleDB Scan Performance Benchmark" << std::endl;
    std::cout << "========================================" << std::endl;
    
    ScanBenchConfig config;
    
    // Parse arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--rows" && i + 1 < argc) {
            config.num_rows = std::stoull(argv[++i]);
        } else if (arg == "--no-sparse-index") {
            config.enable_sparse_index = false;
        } else if (arg == "--reverse") {
            config.reverse_order = true;
        }
    }
    
    std::cout << "\nCreating test SSTable with " << config.num_rows << " rows..." << std::endl;
    
    // Generate test data and create SSTable
    std::vector<BenchRecord> records;
    records.reserve(config.num_rows);
    
    int64_t base_timestamp = 1700000000000;
    std::mt19937_64 gen(42);
    std::uniform_int_distribution<int64_t> user_dist(1, 100000);
    std::uniform_real_distribution<double> amount_dist(1.0, 10000.0);
    std::vector<std::string> categories = {"groceries", "entertainment", "bills", "transfer", "other"};
    
    for (size_t i = 0; i < config.num_rows; ++i) {
        BenchRecord record;
        record.id = i;
        record.timestamp = base_timestamp + (i * 1000);
        record.user_id = user_dist(gen);
        record.amount = amount_dist(gen);
        record.category = categories[i % categories.size()];
        records.push_back(record);
    }
    
    // Build Arrow RecordBatch
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
    
    // Create SSTable with indexing
    DBOptions options;
    options.enable_sparse_index = config.enable_sparse_index;
    options.index_granularity = config.index_granularity;
    options.target_block_size = config.index_granularity;
    options.enable_bloom_filter = true;
    
    std::unique_ptr<LSMSSTable> sstable;
    auto status = CreateSSTable(config.sstable_path, batch, options, &sstable);
    
    if (!status.ok()) {
        std::cerr << "Failed to create SSTable: " << status.ToString() << std::endl;
        return 1;
    }
    
    const auto& metadata = sstable->GetMetadata();
    std::cout << "SSTable created:" << std::endl;
    std::cout << "  Rows: " << metadata.num_entries << std::endl;
    std::cout << "  Blocks: " << metadata.block_stats.size() << std::endl;
    std::cout << "  Sparse index entries: " << metadata.sparse_index.size() << std::endl;
    std::cout << "  Bloom filter: " << (metadata.has_bloom_filter ? "yes" : "no") << std::endl;
    
    // Run benchmarks
    BenchmarkFullScan(sstable, config);
    BenchmarkRangeScan(sstable, config);
    BenchmarkReverseScan(sstable, config);
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "  Scan Benchmarks Complete" << std::endl;
    std::cout << "========================================" << std::endl;
    
    // Cleanup
    std::remove(config.sstable_path.c_str());
    
    return 0;
}

