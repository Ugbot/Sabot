/************************************************************************
MarbleDB Write Performance Benchmark

Tests write throughput for LSMSSTable with ClickHouse-style indexing.
Compares against Tonbo's write performance baseline.
**************************************************************************/

#include <marble/marble.h>
#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <iomanip>
#include <sstream>

using namespace marble;

// Benchmark configuration
struct BenchConfig {
    size_t num_rows = 1000000;  // 1M rows default
    size_t batch_size = 1000;
    bool enable_wal = true;
    bool enable_compression = true;
    bool enable_sparse_index = true;
    size_t index_granularity = 8192;
    std::string output_file = "results/marble_write.json";
};

// Simple record for benchmarking
class BenchKey : public Key {
public:
    explicit BenchKey(int64_t id) : id_(id) {}
    
    int Compare(const Key& other) const override {
        const auto& other_key = static_cast<const BenchKey&>(other);
        if (id_ < other_key.id_) return -1;
        if (id_ > other_key.id_) return 1;
        return 0;
    }
    
    std::shared_ptr<Key> Clone() const override {
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

struct BenchRecord {
    int64_t id;
    int64_t timestamp;
    int64_t user_id;
    double amount;
    std::string category;
};

class BenchRecordImpl : public Record {
public:
    explicit BenchRecordImpl(BenchRecord record) : record_(std::move(record)) {}
    
    std::shared_ptr<Key> GetKey() const override {
        return std::make_shared<BenchKey>(record_.id);
    }
    
    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("timestamp", arrow::int64()),
            arrow::field("user_id", arrow::int64()),
            arrow::field("amount", arrow::float64()),
            arrow::field("category", arrow::utf8())
        });
    }
    
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override {
        arrow::Int64Builder id_builder, ts_builder, user_builder;
        arrow::DoubleBuilder amount_builder;
        arrow::StringBuilder category_builder;
        
        ARROW_RETURN_NOT_OK(id_builder.Append(record_.id));
        ARROW_RETURN_NOT_OK(ts_builder.Append(record_.timestamp));
        ARROW_RETURN_NOT_OK(user_builder.Append(record_.user_id));
        ARROW_RETURN_NOT_OK(amount_builder.Append(record_.amount));
        ARROW_RETURN_NOT_OK(category_builder.Append(record_.category));
        
        std::shared_ptr<arrow::Array> id_array, ts_array, user_array, amount_array, category_array;
        ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
        ARROW_RETURN_NOT_OK(ts_builder.Finish(&ts_array));
        ARROW_RETURN_NOT_OK(user_builder.Finish(&user_array));
        ARROW_RETURN_NOT_OK(amount_builder.Finish(&amount_array));
        ARROW_RETURN_NOT_OK(category_builder.Finish(&category_array));
        
        return arrow::RecordBatch::Make(GetArrowSchema(), 1,
            {id_array, ts_array, user_array, amount_array, category_array});
    }
    
    std::unique_ptr<RecordRef> AsRecordRef() const override {
        return nullptr;
    }
    
private:
    BenchRecord record_;
};

// Generate test data
std::vector<BenchRecord> GenerateTestData(size_t num_rows) {
    std::vector<BenchRecord> records;
    records.reserve(num_rows);
    
    std::mt19937_64 gen(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<int64_t> user_dist(1, 100000);
    std::uniform_real_distribution<double> amount_dist(1.0, 10000.0);
    std::uniform_int_distribution<int> category_dist(0, 4);
    
    std::vector<std::string> categories = {"groceries", "entertainment", "bills", "transfer", "other"};
    
    int64_t base_timestamp = 1700000000000; // Nov 2023
    
    for (size_t i = 0; i < num_rows; ++i) {
        BenchRecord record;
        record.id = i;
        record.timestamp = base_timestamp + (i * 1000); // 1 second apart
        record.user_id = user_dist(gen);
        record.amount = amount_dist(gen);
        record.category = categories[category_dist(gen)];
        records.push_back(record);
    }
    
    return records;
}

// Convert records to Arrow RecordBatch
arrow::Result<std::shared_ptr<arrow::RecordBatch>> RecordsToArrowBatch(
    const std::vector<BenchRecord>& records) {
    
    arrow::Int64Builder id_builder, ts_builder, user_builder;
    arrow::DoubleBuilder amount_builder;
    arrow::StringBuilder category_builder;
    
    for (const auto& record : records) {
        ARROW_RETURN_NOT_OK(id_builder.Append(record.id));
        ARROW_RETURN_NOT_OK(ts_builder.Append(record.timestamp));
        ARROW_RETURN_NOT_OK(user_builder.Append(record.user_id));
        ARROW_RETURN_NOT_OK(amount_builder.Append(record.amount));
        ARROW_RETURN_NOT_OK(category_builder.Append(record.category));
    }
    
    std::shared_ptr<arrow::Array> id_array, ts_array, user_array, amount_array, category_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    ARROW_RETURN_NOT_OK(ts_builder.Finish(&ts_array));
    ARROW_RETURN_NOT_OK(user_builder.Finish(&user_array));
    ARROW_RETURN_NOT_OK(amount_builder.Finish(&amount_array));
    ARROW_RETURN_NOT_OK(category_builder.Finish(&category_array));
    
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("user_id", arrow::int64()),
        arrow::field("amount", arrow::float64()),
        arrow::field("category", arrow::utf8())
    });
    
    return arrow::RecordBatch::Make(schema, records.size(),
        {id_array, ts_array, user_array, amount_array, category_array});
}

// Benchmark: Batch write to SSTable
void BenchmarkBatchWrite(const BenchConfig& config) {
    std::cout << "\n=== Benchmark: Batch Write to LSMSSTable ===" << std::endl;
    std::cout << "Rows: " << config.num_rows << ", Batch size: " << config.batch_size << std::endl;
    
    // Generate test data
    std::cout << "Generating " << config.num_rows << " test records..." << std::endl;
    auto start_gen = std::chrono::high_resolution_clock::now();
    auto records = GenerateTestData(config.num_rows);
    auto end_gen = std::chrono::high_resolution_clock::now();
    auto gen_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_gen - start_gen);
    std::cout << "Data generation took " << gen_duration.count() << " ms" << std::endl;
    
    // Setup DB options with ClickHouse indexing
    DBOptions options;
    options.enable_sparse_index = config.enable_sparse_index;
    options.index_granularity = config.index_granularity;
    options.target_block_size = config.index_granularity;
    options.enable_bloom_filter = true;
    options.bloom_filter_bits_per_key = 10;
    
    // Benchmark: Write in batches
    std::cout << "\nWriting data in batches of " << config.batch_size << "..." << std::endl;
    
    size_t num_batches = (config.num_rows + config.batch_size - 1) / config.batch_size;
    size_t total_bytes_written = 0;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx) {
        size_t start_idx = batch_idx * config.batch_size;
        size_t end_idx = std::min(start_idx + config.batch_size, config.num_rows);
        
        std::vector<BenchRecord> batch_records(
            records.begin() + start_idx,
            records.begin() + end_idx
        );
        
        // Convert to Arrow RecordBatch
        auto batch_result = RecordsToArrowBatch(batch_records);
        if (!batch_result.ok()) {
            std::cerr << "Failed to create batch: " << batch_result.status().ToString() << std::endl;
            continue;
        }
        auto batch = batch_result.ValueUnsafe();
        
        // Write to SSTable (simulated - one SSTable per batch for benchmark)
        std::string filename = "/tmp/marble_bench_" + std::to_string(batch_idx) + ".sst";
        std::unique_ptr<LSMSSTable> sstable;
        auto status = CreateSSTable(filename, batch, options, &sstable);
        
        if (!status.ok()) {
            std::cerr << "Failed to create SSTable: " << status.ToString() << std::endl;
            continue;
        }
        
        total_bytes_written += batch->num_rows() * 64; // Estimate 64 bytes per row
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Calculate metrics
    double duration_sec = duration.count() / 1000.0;
    double throughput_rows = config.num_rows / duration_sec;
    double throughput_mb = (total_bytes_written / (1024.0 * 1024.0)) / duration_sec;
    
    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "Total time: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput_rows << " rows/sec" << std::endl;
    std::cout << "Throughput: " << throughput_mb << " MB/sec" << std::endl;
    std::cout << "Latency per row: " << (duration.count() * 1000000.0 / config.num_rows) << " ns" << std::endl;
    std::cout << "Batches written: " << num_batches << std::endl;
    
}

// Benchmark: Single record writes (MemTable)
void BenchmarkSingleWrites(const BenchConfig& config) {
    std::cout << "\n=== Benchmark: Single Record Writes (MemTable) ===" << std::endl;
    
    // Create memtable
    auto schema = std::make_shared<MockSchema>();
    auto memtable = CreateSkipListMemTable(schema);
    
    // Generate small dataset for single writes
    size_t num_writes = std::min(config.num_rows, size_t(100000)); // Cap at 100K for single writes
    auto records = GenerateTestData(num_writes);
    
    std::cout << "Writing " << num_writes << " individual records to MemTable..." << std::endl;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (const auto& bench_record : records) {
        auto key = std::make_shared<BenchKey>(bench_record.id);
        auto record = std::make_shared<BenchRecordImpl>(bench_record);
        auto status = memtable->Put(key, record);
        
        if (!status.ok() && status.code() != StatusCode::kResourceExhausted) {
            std::cerr << "Put failed: " << status.ToString() << std::endl;
            break;
        }
        
        // MemTable full - in real scenario would flush
        if (status.code() == StatusCode::kResourceExhausted) {
            break;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    size_t actual_writes = memtable->Size();
    double duration_sec = duration.count() / 1000.0;
    double throughput = actual_writes / duration_sec;
    
    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "Records written: " << actual_writes << std::endl;
    std::cout << "Total time: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(2) << throughput << " rows/sec" << std::endl;
    std::cout << "Latency per write: " << (duration.count() * 1000000.0 / actual_writes) << " ns" << std::endl;
    
}

// Mock Schema implementation
class MockSchema : public Schema {
public:
    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("timestamp", arrow::int64()),
            arrow::field("user_id", arrow::int64()),
            arrow::field("amount", arrow::float64()),
            arrow::field("category", arrow::utf8())
        });
    }
    
    const std::vector<size_t>& GetPrimaryKeyIndices() const override {
        static const std::vector<size_t> indices = {0};
        return indices;
    }
    
    arrow::Result<std::shared_ptr<Key>> KeyFromScalar(const arrow::Scalar& scalar) const override {
        if (scalar.type->id() == arrow::Type::INT64) {
            const auto& int_scalar = static_cast<const arrow::Int64Scalar&>(scalar);
            if (int_scalar.is_valid) {
                return std::make_shared<BenchKey>(int_scalar.value);
            }
        }
        return arrow::Status::Invalid("Invalid scalar type for key");
    }
    
    std::unique_ptr<RecordRef> AsRecordRef() const override {
        return nullptr;
    }
};

int main(int argc, char* argv[]) {
    std::cout << "========================================" << std::endl;
    std::cout << "  MarbleDB Write Performance Benchmark" << std::endl;
    std::cout << "========================================" << std::endl;
    
    BenchConfig config;
    
    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--rows" && i + 1 < argc) {
            config.num_rows = std::stoull(argv[++i]);
        } else if (arg == "--batch-size" && i + 1 < argc) {
            config.batch_size = std::stoull(argv[++i]);
        } else if (arg == "--no-sparse-index") {
            config.enable_sparse_index = false;
        } else if (arg == "--index-granularity" && i + 1 < argc) {
            config.index_granularity = std::stoull(argv[++i]);
        }
    }
    
    std::cout << "\nConfiguration:" << std::endl;
    std::cout << "  Rows: " << config.num_rows << std::endl;
    std::cout << "  Batch size: " << config.batch_size << std::endl;
    std::cout << "  Sparse index: " << (config.enable_sparse_index ? "enabled" : "disabled") << std::endl;
    std::cout << "  Index granularity: " << config.index_granularity << std::endl;
    
    // Run benchmarks
    BenchmarkBatchWrite(config);
    std::cout << "\n" << std::string(60, '-') << "\n" << std::endl;
    BenchmarkSingleWrites(config);
    
    std::cout << "\n========================================" << std::endl;
    std::cout << "  Benchmark Complete" << std::endl;
    std::cout << "========================================" << std::endl;
    
    // Cleanup
    std::cout << "\nCleaning up test files..." << std::endl;
    system("rm -f /tmp/marble_bench_*.sst");
    
    return 0;
}
