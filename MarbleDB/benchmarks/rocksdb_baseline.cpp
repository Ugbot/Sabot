/**
 * @file rocksdb_baseline.cpp
 * @brief RocksDB performance baseline for comparison with MarbleDB
 *
 * This benchmark establishes RocksDB performance baselines that can be
 * compared with existing MarbleDB benchmarks to validate the improvements
 * from skipping indexes, bloom filters, and index persistence.
 *
 * Build:
 *   cd /Users/bengamble/Sabot/MarbleDB/build
 *   cmake ..
 *   make rocksdb_baseline
 *
 * Run:
 *   ./rocksdb_baseline
 */

#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <random>
#include <algorithm>
#include <sstream>
#include <cstring>

// RocksDB headers
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/iterator.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

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
        std::cout << "  " << std::left << std::setw(45) << name;

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

        std::cout << std::setw(15) << std::fixed << std::setprecision(3)
                 << latency_us << " μs/op" << std::endl;
    }
};

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
// RocksDB Benchmark
//==============================================================================

class RocksDBBenchmark {
public:
    RocksDBBenchmark(const std::string& db_path) : db_path_(db_path), db_(nullptr) {}

    ~RocksDBBenchmark() {
        Close();
    }

    bool Open(bool with_bloom = true) {
        CleanDirectory(db_path_);

        rocksdb::Options options;
        options.create_if_missing = true;
        options.write_buffer_size = 64 * 1024 * 1024;  // 64MB memtable
        options.max_write_buffer_number = 3;
        options.target_file_size_base = 64 * 1024 * 1024;
        options.max_background_jobs = 4;
        options.compression = rocksdb::kNoCompression;  // For fair comparison

        if (with_bloom) {
            // Enable bloom filter (similar to MarbleDB)
            rocksdb::BlockBasedTableOptions table_options;
            table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
            table_options.block_size = 8 * 1024;  // 8KB blocks
            table_options.cache_index_and_filter_blocks = true;
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

    BenchmarkResult RunSequentialWrites(size_t num_keys, size_t value_size = 512) {
        BenchmarkResult result;
        result.name = "Sequential Writes (" + std::to_string(num_keys) + " keys)";
        result.operations = num_keys;

        Timer timer;

        for (size_t i = 0; i < num_keys; ++i) {
            std::string key = std::to_string(i);
            std::string value = GenerateValue(value_size);
            rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
            if (!status.ok()) {
                std::cerr << "Put failed: " << status.ToString() << std::endl;
            }
        }

        result.duration_ms = timer.ElapsedMs();
        result.ops_per_sec = (result.operations / result.duration_ms) * 1000.0;
        result.latency_us = (result.duration_ms * 1000.0) / result.operations;

        return result;
    }

    BenchmarkResult RunPointLookups(size_t num_lookups, size_t key_range) {
        BenchmarkResult result;
        result.name = "Point Lookups (" + std::to_string(num_lookups) + " ops)";
        result.operations = num_lookups;

        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dist(0, key_range - 1);

        Timer timer;
        size_t found = 0;

        for (size_t i = 0; i < num_lookups; ++i) {
            uint64_t key_id = dist(gen);
            std::string key = std::to_string(key_id);
            std::string value;

            rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
            if (status.ok()) {
                found++;
            }
        }

        result.duration_ms = timer.ElapsedMs();
        result.ops_per_sec = (result.operations / result.duration_ms) * 1000.0;
        result.latency_us = (result.duration_ms * 1000.0) / result.operations;

        std::cout << "  (found: " << found << "/" << num_lookups << ")" << std::endl;

        return result;
    }

    BenchmarkResult RunRangeScan(size_t expected_rows) {
        BenchmarkResult result;
        result.name = "Range Scan (full table)";

        Timer timer;
        size_t count = 0;

        rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            count++;
        }

        delete it;

        result.duration_ms = timer.ElapsedMs();
        result.operations = count;
        result.ops_per_sec = (count / result.duration_ms) * 1000.0;
        result.latency_us = count > 0 ? (result.duration_ms * 1000.0) / count : 0.0;

        std::cout << "  (scanned: " << count << " rows)" << std::endl;

        return result;
    }

    BenchmarkResult RunRestartBenchmark() {
        BenchmarkResult result;
        result.name = "Database Restart";
        result.operations = 1;

        Timer timer;
        Reopen();
        result.duration_ms = timer.ElapsedMs();
        result.ops_per_sec = 1000.0 / result.duration_ms;
        result.latency_us = result.duration_ms * 1000.0;

        return result;
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
// Main
//==============================================================================

int main() {
    std::cout << "=====================================================================" << std::endl;
    std::cout << "RocksDB Performance Baseline" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    // Configuration
    const size_t NUM_KEYS = 100000;
    const size_t NUM_LOOKUPS = 10000;
    const std::string DB_PATH = "/tmp/rocksdb_baseline";

    std::cout << "Configuration:" << std::endl;
    std::cout << "  Dataset size: " << NUM_KEYS << " keys" << std::endl;
    std::cout << "  Lookup queries: " << NUM_LOOKUPS << std::endl;
    std::cout << "  Value size: 512 bytes" << std::endl;
    std::cout << "  Bloom filters: Enabled" << std::endl;
    std::cout << "  Block size: 8KB (for skipping comparison)" << std::endl;
    std::cout << std::endl;

    RocksDBBenchmark rocks(DB_PATH);

    std::cout << "Opening database..." << std::endl;
    if (!rocks.Open(true)) {
        std::cerr << "Failed to open RocksDB" << std::endl;
        return 1;
    }
    std::cout << "Database opened successfully." << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // 1. WRITE PERFORMANCE
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 1. Write Performance                                                    │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                                   │ Throughput  │ Latency     │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto write_result = rocks.RunSequentialWrites(NUM_KEYS, 512);
    write_result.Print();

    std::cout << "└─────────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    // Flush to disk
    std::cout << "Flushing to disk..." << std::endl;
    rocks.Flush();
    std::cout << std::endl;

    //==========================================================================
    // 2. POINT LOOKUP PERFORMANCE
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 2. Point Lookup Performance (Bloom Filter Benefit)                     │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                                   │ Throughput  │ Latency     │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto lookup_result = rocks.RunPointLookups(NUM_LOOKUPS, NUM_KEYS);
    lookup_result.Print();

    std::cout << "└─────────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // 3. RANGE SCAN PERFORMANCE
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 3. Range Scan Performance                                               │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                                   │ Throughput  │ Latency     │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto scan_result = rocks.RunRangeScan(NUM_KEYS);
    scan_result.Print();

    std::cout << "└─────────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // 4. DATABASE RESTART
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 4. Database Restart Performance                                         │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                                   │ Throughput  │ Latency     │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto restart_result = rocks.RunRestartBenchmark();
    restart_result.Print();

    std::cout << "└─────────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // SUMMARY
    //==========================================================================

    std::cout << "=====================================================================" << std::endl;
    std::cout << "Summary - RocksDB Baseline Results" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    std::cout << "These baseline results can be compared with MarbleDB benchmarks" << std::endl;
    std::cout << "to measure the benefit of recent improvements:" << std::endl;
    std::cout << "  - Bloom filter persistence (helps point lookups)" << std::endl;
    std::cout << "  - Skipping index persistence (helps range scans)" << std::endl;
    std::cout << "  - Index auto-loading on restart (helps restart time)" << std::endl;
    std::cout << std::endl;

    std::cout << "RocksDB Results:" << std::endl;
    std::cout << "  Write throughput:    " << std::fixed << std::setprecision(2)
             << (write_result.ops_per_sec / 1000.0) << " K ops/sec" << std::endl;
    std::cout << "  Point lookup:        " << std::fixed << std::setprecision(3)
             << lookup_result.latency_us << " μs/op" << std::endl;
    std::cout << "  Range scan:          " << std::fixed << std::setprecision(2)
             << (scan_result.ops_per_sec / 1000000.0) << " M rows/sec" << std::endl;
    std::cout << "  Database restart:    " << std::fixed << std::setprecision(2)
             << restart_result.latency_us / 1000.0 << " ms" << std::endl;
    std::cout << std::endl;

    std::cout << "Next: Run MarbleDB benchmarks with:" << std::endl;
    std::cout << "  ./marble_bench_simple" << std::endl;
    std::cout << "  ./marble_bench_harness --benchmark all --size medium" << std::endl;
    std::cout << std::endl;

    rocks.Close();

    std::cout << "Benchmark complete!" << std::endl;

    return 0;
}
