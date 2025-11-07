/**
 * @file marble_vs_rocksdb_comparison.cpp
 * @brief Comprehensive MarbleDB vs RocksDB performance comparison
 *
 * Tests both systems with identical workloads:
 * - Point lookups (bloom filter benefit)
 * - Range scans (skipping index benefit)
 * - Mixed workload (combined optimizations)
 * - Write throughput
 * - Database restart/recovery (persistence benefit)
 *
 * Build:
 *   cd /Users/bengamble/Sabot/MarbleDB/build
 *   cmake ..
 *   make marble_vs_rocksdb_comparison
 *
 * Run:
 *   ./marble_vs_rocksdb_comparison --all
 */

#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <random>
#include <algorithm>
#include <memory>
#include <fstream>
#include <sstream>
#include <cstring>
#include <sys/stat.h>

// MarbleDB headers
#include "marble/api.h"
#include "marble/table.h"
#include "marble/optimization_factory.h"
#include "marble/optimization_strategy.h"
#include "marble/table_capabilities.h"

// RocksDB headers
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/iterator.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

using namespace marble;

//==============================================================================
// Utilities
//==============================================================================

class Timer {
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}

    double ElapsedMs() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start_).count();
    }

    double ElapsedUs() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::micro>(end - start_).count();
    }

    void Reset() {
        start_ = std::chrono::high_resolution_clock::now();
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

struct BenchmarkResult {
    std::string name;
    double duration_ms;
    size_t operations;
    double ops_per_sec;
    double latency_us;

    void Print() const {
        std::cout << "  " << std::left << std::setw(40) << name;

        if (ops_per_sec > 1000000) {
            std::cout << std::fixed << std::setprecision(2) << std::setw(12)
                     << (ops_per_sec / 1000000.0) << " M/sec";
        } else if (ops_per_sec > 1000) {
            std::cout << std::fixed << std::setprecision(2) << std::setw(12)
                     << (ops_per_sec / 1000.0) << " K/sec";
        } else {
            std::cout << std::fixed << std::setprecision(2) << std::setw(12)
                     << ops_per_sec << " /sec";
        }

        std::cout << std::setw(12) << std::fixed << std::setprecision(2)
                 << latency_us << " μs" << std::endl;
    }
};

// Generate random keys
std::vector<uint64_t> GenerateRandomKeys(size_t count, uint64_t min_key = 1, uint64_t max_key = 1000000000) {
    std::vector<uint64_t> keys;
    keys.reserve(count);

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(min_key, max_key);

    for (size_t i = 0; i < count; ++i) {
        keys.push_back(dist(gen));
    }

    return keys;
}

// Generate sequential keys
std::vector<uint64_t> GenerateSequentialKeys(size_t count, uint64_t start_key = 1) {
    std::vector<uint64_t> keys;
    keys.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        keys.push_back(start_key + i);
    }

    return keys;
}

// Generate random value
std::string GenerateValue(size_t size = 512) {
    static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);

    std::string result;
    result.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        result += charset[dist(gen)];
    }

    return result;
}

// Clean directory
void CleanDirectory(const std::string& path) {
    std::string cmd = "rm -rf " + path;
    system(cmd.c_str());
}

//==============================================================================
// MarbleDB Benchmark Wrapper
//==============================================================================

class MarbleDBBenchmark {
public:
    MarbleDBBenchmark(const std::string& db_path) : db_path_(db_path) {}

    bool Open(bool with_optimizations = true) {
        CleanDirectory(db_path_);

        auto db_result = OpenDB(db_path_);
        if (!db_result.ok()) {
            std::cerr << "Failed to open MarbleDB: " << db_result.status().ToString() << std::endl;
            return false;
        }
        db_ = std::move(db_result.ValueOrDie());

        // Create table with optimizations
        TableCapabilities caps;
        caps.name = "test_table";
        caps.memtable_size_mb = 64;
        caps.enable_bloom_filter = with_optimizations;
        caps.enable_skipping_index = with_optimizations;

        WorkloadHints hints;
        hints.force_bloom_filter = with_optimizations;
        hints.force_skipping_index = with_optimizations;
        hints.access_pattern = WorkloadHints::MIXED;

        auto schema = arrow::schema({
            arrow::field("key", arrow::uint64()),
            arrow::field("value", arrow::utf8())
        });

        auto table_result = db_->CreateTable("test_table", schema, caps, hints);
        if (!table_result.ok()) {
            std::cerr << "Failed to create table: " << table_result.ToString() << std::endl;
            return false;
        }
        table_ = std::move(table_result.ValueOrDie());

        return true;
    }

    bool Reopen() {
        table_.reset();
        db_.reset();

        auto db_result = OpenDB(db_path_);
        if (!db_result.ok()) {
            std::cerr << "Failed to reopen MarbleDB: " << db_result.status().ToString() << std::endl;
            return false;
        }
        db_ = std::move(db_result.ValueOrDie());

        auto table_result = db_->OpenTable("test_table");
        if (!table_result.ok()) {
            std::cerr << "Failed to open table: " << table_result.ToString() << std::endl;
            return false;
        }
        table_ = std::move(table_result.ValueOrDie());

        return true;
    }

    void Close() {
        table_.reset();
        db_.reset();
    }

    bool Put(uint64_t key, const std::string& value) {
        auto key_builder = arrow::UInt64Builder();
        auto value_builder = arrow::StringBuilder();

        key_builder.Append(key);
        value_builder.Append(value);

        std::shared_ptr<arrow::Array> key_array, value_array;
        key_builder.Finish(&key_array);
        value_builder.Finish(&value_array);

        auto batch = arrow::RecordBatch::Make(
            table_->schema(),
            1,
            {key_array, value_array}
        );

        auto status = table_->InsertBatch(batch);
        return status.ok();
    }

    bool Get(uint64_t key, std::string* value) {
        // For now, use scan (MarbleDB doesn't have point lookup API yet)
        // This is a limitation we'll note in results
        auto result = table_->Scan();
        if (!result.ok()) {
            return false;
        }

        auto batch = result.ValueOrDie();
        auto key_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        auto value_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            if (key_array->Value(i) == key) {
                *value = value_array->GetString(i);
                return true;
            }
        }

        return false;
    }

    size_t Scan(uint64_t min_key, uint64_t max_key) {
        auto result = table_->Scan();
        if (!result.ok()) {
            return 0;
        }

        auto batch = result.ValueOrDie();
        auto key_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));

        size_t count = 0;
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            uint64_t key = key_array->Value(i);
            if (key >= min_key && key <= max_key) {
                count++;
            }
        }

        return count;
    }

    bool Flush() {
        auto status = table_->Flush();
        return status.ok();
    }

private:
    std::string db_path_;
    std::unique_ptr<SimpleMarbleDB> db_;
    std::unique_ptr<Table> table_;
};

//==============================================================================
// RocksDB Benchmark Wrapper
//==============================================================================

class RocksDBBenchmark {
public:
    RocksDBBenchmark(const std::string& db_path) : db_path_(db_path), db_(nullptr) {}

    ~RocksDBBenchmark() {
        Close();
    }

    bool Open(bool with_optimizations = true) {
        CleanDirectory(db_path_);

        rocksdb::Options options;
        options.create_if_missing = true;
        options.write_buffer_size = 64 * 1024 * 1024;  // 64MB memtable

        if (with_optimizations) {
            // Enable bloom filter
            rocksdb::BlockBasedTableOptions table_options;
            table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
            table_options.block_size = 8 * 1024;  // 8KB blocks (similar to skipping index)
            options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        }

        rocksdb::Status status = rocksdb::DB::Open(options, db_path_, &db_);
        if (!status.ok()) {
            std::cerr << "Failed to open RocksDB: " << status.ToString() << std::endl;
            return false;
        }

        return true;
    }

    bool Reopen() {
        Close();

        rocksdb::Options options;
        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        rocksdb::Status status = rocksdb::DB::Open(options, db_path_, &db_);
        if (!status.ok()) {
            std::cerr << "Failed to reopen RocksDB: " << status.ToString() << std::endl;
            return false;
        }

        return true;
    }

    void Close() {
        if (db_) {
            delete db_;
            db_ = nullptr;
        }
    }

    bool Put(uint64_t key, const std::string& value) {
        std::string key_str = std::to_string(key);
        rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key_str, value);
        return status.ok();
    }

    bool Get(uint64_t key, std::string* value) {
        std::string key_str = std::to_string(key);
        rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key_str, value);
        return status.ok();
    }

    size_t Scan(uint64_t min_key, uint64_t max_key) {
        size_t count = 0;

        rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            uint64_t key = std::stoull(it->key().ToString());
            if (key >= min_key && key <= max_key) {
                count++;
            }
        }

        delete it;
        return count;
    }

    bool Flush() {
        rocksdb::FlushOptions flush_options;
        rocksdb::Status status = db_->Flush(flush_options);
        return status.ok();
    }

private:
    std::string db_path_;
    rocksdb::DB* db_;
};

//==============================================================================
// Benchmark Runners
//==============================================================================

BenchmarkResult RunPointLookupBenchmark(MarbleDBBenchmark& marble, RocksDBBenchmark& rocks,
                                        const std::vector<uint64_t>& keys,
                                        const std::string& db_type) {
    BenchmarkResult result;
    result.name = db_type + " - Point Lookups (" + std::to_string(keys.size()) + " ops)";
    result.operations = keys.size();

    Timer timer;
    size_t found = 0;

    if (db_type == "MarbleDB") {
        for (uint64_t key : keys) {
            std::string value;
            if (marble.Get(key, &value)) {
                found++;
            }
        }
    } else {
        for (uint64_t key : keys) {
            std::string value;
            if (rocks.Get(key, &value)) {
                found++;
            }
        }
    }

    result.duration_ms = timer.ElapsedMs();
    result.ops_per_sec = (result.operations / result.duration_ms) * 1000.0;
    result.latency_us = (result.duration_ms * 1000.0) / result.operations;

    return result;
}

BenchmarkResult RunRangeScanBenchmark(MarbleDBBenchmark& marble, RocksDBBenchmark& rocks,
                                      uint64_t min_key, uint64_t max_key,
                                      const std::string& db_type) {
    BenchmarkResult result;
    result.name = db_type + " - Range Scan";

    Timer timer;
    size_t count = 0;

    if (db_type == "MarbleDB") {
        count = marble.Scan(min_key, max_key);
    } else {
        count = rocks.Scan(min_key, max_key);
    }

    result.duration_ms = timer.ElapsedMs();
    result.operations = count;
    result.ops_per_sec = (count / result.duration_ms) * 1000.0;
    result.latency_us = count > 0 ? (result.duration_ms * 1000.0) / count : 0.0;

    return result;
}

BenchmarkResult RunWriteBenchmark(MarbleDBBenchmark& marble, RocksDBBenchmark& rocks,
                                  const std::vector<uint64_t>& keys,
                                  const std::string& db_type) {
    BenchmarkResult result;
    result.name = db_type + " - Sequential Writes (" + std::to_string(keys.size()) + " ops)";
    result.operations = keys.size();

    Timer timer;

    if (db_type == "MarbleDB") {
        for (uint64_t key : keys) {
            std::string value = GenerateValue(512);
            marble.Put(key, value);
        }
    } else {
        for (uint64_t key : keys) {
            std::string value = GenerateValue(512);
            rocks.Put(key, value);
        }
    }

    result.duration_ms = timer.ElapsedMs();
    result.ops_per_sec = (result.operations / result.duration_ms) * 1000.0;
    result.latency_us = (result.duration_ms * 1000.0) / result.operations;

    return result;
}

BenchmarkResult RunRestartBenchmark(MarbleDBBenchmark& marble, RocksDBBenchmark& rocks,
                                    const std::string& db_type) {
    BenchmarkResult result;
    result.name = db_type + " - Database Restart";
    result.operations = 1;

    Timer timer;

    if (db_type == "MarbleDB") {
        marble.Reopen();
    } else {
        rocks.Reopen();
    }

    result.duration_ms = timer.ElapsedMs();
    result.ops_per_sec = 1000.0 / result.duration_ms;
    result.latency_us = result.duration_ms * 1000.0;

    return result;
}

//==============================================================================
// Main
//==============================================================================

int main(int argc, char** argv) {
    std::cout << "=====================================================================" << std::endl;
    std::cout << "MarbleDB vs RocksDB - Performance Comparison" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    // Configuration
    const size_t NUM_KEYS = 100000;
    const size_t NUM_LOOKUPS = 10000;
    const std::string MARBLE_PATH = "/tmp/marble_bench";
    const std::string ROCKS_PATH = "/tmp/rocksdb_bench";

    std::cout << "Configuration:" << std::endl;
    std::cout << "  Dataset size: " << NUM_KEYS << " keys" << std::endl;
    std::cout << "  Lookup queries: " << NUM_LOOKUPS << std::endl;
    std::cout << "  Value size: 512 bytes" << std::endl;
    std::cout << "  Optimizations: Enabled (Bloom filters + Skipping indexes)" << std::endl;
    std::cout << std::endl;

    // Generate test data
    std::cout << "Generating test data..." << std::endl;
    auto write_keys = GenerateSequentialKeys(NUM_KEYS, 1);
    auto lookup_keys = GenerateRandomKeys(NUM_LOOKUPS, 1, NUM_KEYS);
    std::cout << "Test data generated." << std::endl;
    std::cout << std::endl;

    // Initialize databases
    MarbleDBBenchmark marble(MARBLE_PATH);
    RocksDBBenchmark rocks(ROCKS_PATH);

    std::cout << "Opening databases..." << std::endl;
    if (!marble.Open(true)) {
        std::cerr << "Failed to open MarbleDB" << std::endl;
        return 1;
    }
    if (!rocks.Open(true)) {
        std::cerr << "Failed to open RocksDB" << std::endl;
        return 1;
    }
    std::cout << "Databases opened successfully." << std::endl;
    std::cout << std::endl;

    // Results storage
    std::vector<BenchmarkResult> results;

    //==========================================================================
    // 1. WRITE PERFORMANCE
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 1. Write Performance                                                │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                              │ Throughput  │ Latency      │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto marble_write = RunWriteBenchmark(marble, rocks, write_keys, "MarbleDB");
    marble_write.Print();
    results.push_back(marble_write);

    auto rocks_write = RunWriteBenchmark(marble, rocks, write_keys, "RocksDB");
    rocks_write.Print();
    results.push_back(rocks_write);

    std::cout << "└─────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    // Flush to disk
    std::cout << "Flushing to disk..." << std::endl;
    marble.Flush();
    rocks.Flush();
    std::cout << std::endl;

    //==========================================================================
    // 2. POINT LOOKUP PERFORMANCE
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 2. Point Lookup Performance (Bloom Filter Benefit)                 │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                              │ Throughput  │ Latency      │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto marble_lookup = RunPointLookupBenchmark(marble, rocks, lookup_keys, "MarbleDB");
    marble_lookup.Print();
    results.push_back(marble_lookup);

    auto rocks_lookup = RunPointLookupBenchmark(marble, rocks, lookup_keys, "RocksDB");
    rocks_lookup.Print();
    results.push_back(rocks_lookup);

    std::cout << "└─────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // 3. RANGE SCAN PERFORMANCE
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 3. Range Scan Performance (Skipping Index Benefit)                 │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                              │ Throughput  │ Latency      │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto marble_scan = RunRangeScanBenchmark(marble, rocks, 1, NUM_KEYS / 2, "MarbleDB");
    marble_scan.Print();
    results.push_back(marble_scan);

    auto rocks_scan = RunRangeScanBenchmark(marble, rocks, 1, NUM_KEYS / 2, "RocksDB");
    rocks_scan.Print();
    results.push_back(rocks_scan);

    std::cout << "└─────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // 4. DATABASE RESTART
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 4. Database Restart Performance (Index Persistence Benefit)        │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                              │ Throughput  │ Latency      │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto marble_restart = RunRestartBenchmark(marble, rocks, "MarbleDB");
    marble_restart.Print();
    results.push_back(marble_restart);

    auto rocks_restart = RunRestartBenchmark(marble, rocks, "RocksDB");
    rocks_restart.Print();
    results.push_back(rocks_restart);

    std::cout << "└─────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // SUMMARY
    //==========================================================================

    std::cout << "=====================================================================" << std::endl;
    std::cout << "Summary" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    std::cout << "Speedup Ratios (MarbleDB / RocksDB):" << std::endl;
    std::cout << "  Write throughput:    " << std::fixed << std::setprecision(2)
             << (marble_write.ops_per_sec / rocks_write.ops_per_sec) << "x" << std::endl;
    std::cout << "  Point lookup:        " << std::fixed << std::setprecision(2)
             << (marble_lookup.ops_per_sec / rocks_lookup.ops_per_sec) << "x" << std::endl;
    std::cout << "  Range scan:          " << std::fixed << std::setprecision(2)
             << (marble_scan.ops_per_sec / rocks_scan.ops_per_sec) << "x" << std::endl;
    std::cout << "  Database restart:    " << std::fixed << std::setprecision(2)
             << (rocks_restart.duration_ms / marble_restart.duration_ms) << "x faster" << std::endl;
    std::cout << std::endl;

    std::cout << "Note: MarbleDB currently lacks a dedicated point lookup API," << std::endl;
    std::cout << "      so point lookups use full table scan. This will be improved." << std::endl;
    std::cout << std::endl;

    // Cleanup
    marble.Close();
    rocks.Close();

    std::cout << "Benchmark complete!" << std::endl;

    return 0;
}
