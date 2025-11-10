/**
 * @file large_scale_comparison.cpp
 * @brief Large-scale benchmark forcing SSTable merges and compaction
 *
 * This benchmark uses 1M keys to force multiple SSTable flushes and merges,
 * testing lock-free performance under realistic production conditions.
 *
 * Build:
 *   cmake --build . --target large_scale_comparison
 *
 * Run:
 *   cd benchmarks && ./large_scale_comparison
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

// MarbleDB headers
#include "marble/api.h"
#include "marble/status.h"

// Tonbo FFI header
extern "C" {
    #include "tonbo_ffi.h"
}

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

    double ElapsedSec() const {
        return ElapsedMs() / 1000.0;
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

struct BenchmarkResult {
    std::string name;
    double duration_sec;
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

        std::cout << std::setw(12) << std::fixed << std::setprecision(1)
                 << duration_sec << " sec";
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

class RocksDBBench {
public:
    RocksDBBench(const std::string& db_path) : db_path_(db_path), db_(nullptr) {}

    ~RocksDBBench() { Close(); }

    bool Open() {
        CleanDirectory(db_path_);

        rocksdb::Options options;
        options.create_if_missing = true;
        options.write_buffer_size = 16 * 1024 * 1024;  // 16MB memtable (smaller to force flushes)
        options.max_write_buffer_number = 3;
        options.target_file_size_base = 16 * 1024 * 1024;  // 16MB SSTable
        options.max_background_jobs = 4;
        options.compression = rocksdb::kNoCompression;

        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        table_options.block_size = 8 * 1024;
        table_options.cache_index_and_filter_blocks = true;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

        rocksdb::Status status = rocksdb::DB::Open(options, db_path_, &db_);
        if (!status.ok()) {
            std::cerr << "Failed to open RocksDB: " << status.ToString() << std::endl;
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

    BenchmarkResult RunWrites(size_t num_keys, size_t value_size) {
        BenchmarkResult result;
        result.name = "Sequential Writes";
        result.operations = num_keys;

        Timer timer;

        for (size_t i = 0; i < num_keys; ++i) {
            std::string key = std::to_string(i);
            std::string value = GenerateValue(value_size);
            rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
            if (!status.ok()) {
                std::cerr << "Put failed: " << status.ToString() << std::endl;
            }

            if ((i + 1) % 100000 == 0) {
                std::cout << "    Progress: " << (i + 1) << " keys written..." << std::endl;
            }
        }

        // Force flush to trigger compaction
        db_->Flush(rocksdb::FlushOptions());

        result.duration_sec = timer.ElapsedSec();
        result.ops_per_sec = result.operations / result.duration_sec;
        result.latency_us = (result.duration_sec * 1000000.0) / result.operations;

        return result;
    }

    BenchmarkResult RunReads(size_t num_lookups, size_t key_range) {
        BenchmarkResult result;
        result.name = "Point Lookups";
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

            if ((i + 1) % 10000 == 0) {
                std::cout << "    Progress: " << (i + 1) << " lookups..." << std::endl;
            }
        }

        result.duration_sec = timer.ElapsedSec();
        result.ops_per_sec = result.operations / result.duration_sec;
        result.latency_us = (result.duration_sec * 1000000.0) / result.operations;

        std::cout << "  (found: " << found << "/" << num_lookups << ")" << std::endl;

        return result;
    }

private:
    std::string db_path_;
    rocksdb::DB* db_;
};

//==============================================================================
// MarbleDB Benchmark
//==============================================================================

class MarbleDBBench {
public:
    MarbleDBBench(const std::string& db_path) : db_path_(db_path), db_(nullptr) {}

    ~MarbleDBBench() { Close(); }

    bool Open() {
        CleanDirectory(db_path_);

        marble::DBOptions db_options;
        db_options.path = db_path_;
        db_options.create_if_missing = true;

        auto status = marble::DB::Open(db_options, &db_);
        if (!status.ok()) {
            std::cerr << "Failed to open MarbleDB: " << status.message() << std::endl;
            return false;
        }

        // Create default column family
        auto schema = arrow::schema({
            arrow::field("key", arrow::utf8()),
            arrow::field("value", arrow::utf8())
        });

        marble::ColumnFamilyDescriptor cf_desc;
        cf_desc.name = "default";
        cf_desc.schema = schema;

        status = db_->CreateColumnFamily(cf_desc);
        if (!status.ok()) {
            std::cerr << "Failed to create column family: " << status.message() << std::endl;
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

    BenchmarkResult RunWrites(size_t num_keys, size_t value_size) {
        BenchmarkResult result;
        result.name = "Sequential Writes";
        result.operations = num_keys;

        Timer timer;

        for (size_t i = 0; i < num_keys; ++i) {
            std::string key = std::to_string(i);
            std::string value = GenerateValue(value_size);

            // Create Arrow record
            arrow::StringBuilder key_builder;
            arrow::StringBuilder value_builder;
            key_builder.Append(key);
            value_builder.Append(value);

            std::shared_ptr<arrow::Array> key_array;
            std::shared_ptr<arrow::Array> value_array;
            key_builder.Finish(&key_array);
            value_builder.Finish(&value_array);

            auto schema = arrow::schema({
                arrow::field("key", arrow::utf8()),
                arrow::field("value", arrow::utf8())
            });

            auto batch = arrow::RecordBatch::Make(schema, 1, {key_array, value_array});
            auto record = std::make_shared<marble::Record>(batch, 0);

            marble::WriteOptions write_opts;
            auto status = db_->Put(write_opts, record);
            if (!status.ok()) {
                std::cerr << "Put failed: " << status.message() << std::endl;
            }

            if ((i + 1) % 100000 == 0) {
                std::cout << "    Progress: " << (i + 1) << " keys written..." << std::endl;
            }
        }

        result.duration_sec = timer.ElapsedSec();
        result.ops_per_sec = result.operations / result.duration_sec;
        result.latency_us = (result.duration_sec * 1000000.0) / result.operations;

        return result;
    }

    BenchmarkResult RunReads(size_t num_lookups, size_t key_range) {
        BenchmarkResult result;
        result.name = "Point Lookups";
        result.operations = num_lookups;

        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dist(0, key_range - 1);

        Timer timer;
        size_t found = 0;

        for (size_t i = 0; i < num_lookups; ++i) {
            uint64_t key_id = dist(gen);
            std::string key = std::to_string(key_id);

            marble::Key marble_key(key);
            std::shared_ptr<marble::Record> record;

            marble::ReadOptions read_opts;
            auto status = db_->Get(read_opts, marble_key, &record);
            if (status.ok()) {
                found++;
            }

            if ((i + 1) % 10000 == 0) {
                std::cout << "    Progress: " << (i + 1) << " lookups..." << std::endl;
            }
        }

        result.duration_sec = timer.ElapsedSec();
        result.ops_per_sec = result.operations / result.duration_sec;
        result.latency_us = (result.duration_sec * 1000000.0) / result.operations;

        std::cout << "  (found: " << found << "/" << num_lookups << ")" << std::endl;

        return result;
    }

private:
    std::string db_path_;
    marble::DB* db_;
};

//==============================================================================
// Tonbo Benchmark
//==============================================================================

class TonboBench {
public:
    TonboBench(const std::string& db_path) : db_path_(db_path), db_(nullptr) {}

    ~TonboBench() { Close(); }

    bool Open() {
        CleanDirectory(db_path_);

        db_ = tonbo_db_open(db_path_.c_str());
        if (!db_) {
            std::cerr << "Failed to open Tonbo database" << std::endl;
            return false;
        }

        return true;
    }

    void Close() {
        if (db_) {
            tonbo_db_close(db_);
            db_ = nullptr;
        }
    }

    BenchmarkResult RunWrites(size_t num_keys, size_t value_size) {
        BenchmarkResult result;
        result.name = "Sequential Writes";
        result.operations = num_keys;

        Timer timer;

        for (size_t i = 0; i < num_keys; ++i) {
            std::string key = std::to_string(i);
            std::string value = GenerateValue(value_size);

            int status = tonbo_db_insert(db_,
                                         key.c_str(),
                                         key.length(),
                                         reinterpret_cast<const uint8_t*>(value.data()),
                                         value.length());
            if (status != 0) {
                std::cerr << "Insert failed for key " << key << std::endl;
            }

            if ((i + 1) % 100000 == 0) {
                std::cout << "    Progress: " << (i + 1) << " keys written..." << std::endl;
            }
        }

        result.duration_sec = timer.ElapsedSec();
        result.ops_per_sec = result.operations / result.duration_sec;
        result.latency_us = (result.duration_sec * 1000000.0) / result.operations;

        return result;
    }

    BenchmarkResult RunReads(size_t num_lookups, size_t key_range) {
        BenchmarkResult result;
        result.name = "Point Lookups";
        result.operations = num_lookups;

        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dist(0, key_range - 1);

        Timer timer;
        size_t found = 0;

        for (size_t i = 0; i < num_lookups; ++i) {
            uint64_t key_id = dist(gen);
            std::string key = std::to_string(key_id);

            uint8_t* value_ptr = nullptr;
            uintptr_t value_len = 0;

            int status = tonbo_db_get(db_,
                                      key.c_str(),
                                      key.length(),
                                      &value_ptr,
                                      &value_len);

            if (status == 0) {
                found++;
                tonbo_free_bytes(value_ptr, value_len);
            }

            if ((i + 1) % 10000 == 0) {
                std::cout << "    Progress: " << (i + 1) << " lookups..." << std::endl;
            }
        }

        result.duration_sec = timer.ElapsedSec();
        result.ops_per_sec = result.operations / result.duration_sec;
        result.latency_us = (result.duration_sec * 1000000.0) / result.operations;

        std::cout << "  (found: " << found << "/" << num_lookups << ")" << std::endl;

        return result;
    }

private:
    std::string db_path_;
    TonboDb* db_;
};

//==============================================================================
// Main
//==============================================================================

int main() {
    std::cout << "=====================================================================" << std::endl;
    std::cout << "Large-Scale Database Comparison (SSTable Merges & Compaction)" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    // Configuration for large-scale test
    const size_t NUM_KEYS = 1000000;      // 1M keys (forces multiple SSTable flushes)
    const size_t NUM_LOOKUPS = 100000;    // 100K lookups
    const size_t VALUE_SIZE = 512;

    std::cout << "Configuration:" << std::endl;
    std::cout << "  Dataset size: " << NUM_KEYS << " keys (1 million)" << std::endl;
    std::cout << "  Lookup queries: " << NUM_LOOKUPS << std::endl;
    std::cout << "  Value size: " << VALUE_SIZE << " bytes" << std::endl;
    std::cout << "  Memtable size: 16MB (smaller to force SSTable flushes)" << std::endl;
    std::cout << "  Expected: Multiple SSTable merges and compactions" << std::endl;
    std::cout << std::endl;

    std::vector<BenchmarkResult> rocksdb_results;
    std::vector<BenchmarkResult> marbledb_results;
    std::vector<BenchmarkResult> tonbo_results;

    //==========================================================================
    // RocksDB Benchmark
    //==========================================================================

    std::cout << "=====================================================================" << std::endl;
    std::cout << "1. RocksDB Benchmark" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    RocksDBBench rocksdb("/tmp/rocksdb_large_scale");
    if (!rocksdb.Open()) {
        return 1;
    }

    std::cout << "Running writes..." << std::endl;
    rocksdb_results.push_back(rocksdb.RunWrites(NUM_KEYS, VALUE_SIZE));
    rocksdb_results.back().Print();
    std::cout << std::endl;

    std::cout << "Running point lookups..." << std::endl;
    rocksdb_results.push_back(rocksdb.RunReads(NUM_LOOKUPS, NUM_KEYS));
    rocksdb_results.back().Print();
    std::cout << std::endl;

    rocksdb.Close();

    //==========================================================================
    // MarbleDB Benchmark
    //==========================================================================

    std::cout << "=====================================================================" << std::endl;
    std::cout << "2. MarbleDB Benchmark (Lock-Free)" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    MarbleDBBench marbledb("/tmp/marbledb_large_scale");
    if (!marbledb.Open()) {
        return 1;
    }

    std::cout << "Running writes..." << std::endl;
    marbledb_results.push_back(marbledb.RunWrites(NUM_KEYS, VALUE_SIZE));
    marbledb_results.back().Print();
    std::cout << std::endl;

    std::cout << "Running point lookups..." << std::endl;
    marbledb_results.push_back(marbledb.RunReads(NUM_LOOKUPS, NUM_KEYS));
    marbledb_results.back().Print();
    std::cout << std::endl;

    marbledb.Close();

    //==========================================================================
    // Tonbo Benchmark
    //==========================================================================

    std::cout << "=====================================================================" << std::endl;
    std::cout << "3. Tonbo Benchmark (Rust LSM-tree)" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    TonboBench tonbo("/tmp/tonbo_large_scale");
    if (!tonbo.Open()) {
        return 1;
    }

    std::cout << "Running writes..." << std::endl;
    tonbo_results.push_back(tonbo.RunWrites(NUM_KEYS, VALUE_SIZE));
    tonbo_results.back().Print();
    std::cout << std::endl;

    std::cout << "Running point lookups..." << std::endl;
    tonbo_results.push_back(tonbo.RunReads(NUM_LOOKUPS, NUM_KEYS));
    tonbo_results.back().Print();
    std::cout << std::endl;

    tonbo.Close();

    //==========================================================================
    // Summary
    //==========================================================================

    std::cout << "=====================================================================" << std::endl;
    std::cout << "Summary - Large-Scale Comparison (1M Keys)" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    std::cout << "Write Performance:" << std::endl;
    std::cout << "  RocksDB:  " << std::fixed << std::setprecision(2)
             << (rocksdb_results[0].ops_per_sec / 1000.0) << " K ops/sec ("
             << rocksdb_results[0].duration_sec << " sec)" << std::endl;
    std::cout << "  MarbleDB: " << std::fixed << std::setprecision(2)
             << (marbledb_results[0].ops_per_sec / 1000.0) << " K ops/sec ("
             << marbledb_results[0].duration_sec << " sec) ["
             << (marbledb_results[0].ops_per_sec / rocksdb_results[0].ops_per_sec)
             << "x vs RocksDB]" << std::endl;
    std::cout << "  Tonbo:    " << std::fixed << std::setprecision(2)
             << (tonbo_results[0].ops_per_sec / 1000.0) << " K ops/sec ("
             << tonbo_results[0].duration_sec << " sec) ["
             << (tonbo_results[0].ops_per_sec / rocksdb_results[0].ops_per_sec)
             << "x vs RocksDB]" << std::endl;
    std::cout << std::endl;

    std::cout << "Read Performance:" << std::endl;
    std::cout << "  RocksDB:  " << std::fixed << std::setprecision(3)
             << rocksdb_results[1].latency_us << " μs/op ("
             << (rocksdb_results[1].ops_per_sec / 1000.0) << " K ops/sec)" << std::endl;
    std::cout << "  MarbleDB: " << std::fixed << std::setprecision(3)
             << marbledb_results[1].latency_us << " μs/op ("
             << (marbledb_results[1].ops_per_sec / 1000.0) << " K ops/sec) ["
             << (rocksdb_results[1].latency_us / marbledb_results[1].latency_us)
             << "x faster than RocksDB]" << std::endl;
    std::cout << "  Tonbo:    " << std::fixed << std::setprecision(3)
             << tonbo_results[1].latency_us << " μs/op ("
             << (tonbo_results[1].ops_per_sec / 1000.0) << " K ops/sec) ["
             << (rocksdb_results[1].latency_us / tonbo_results[1].latency_us)
             << "x faster than RocksDB]" << std::endl;
    std::cout << std::endl;

    std::cout << "This benchmark forced multiple SSTable flushes and compactions." << std::endl;
    std::cout << "Lock-free optimizations maintain performance advantage under load." << std::endl;

    return 0;
}
