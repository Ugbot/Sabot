//==============================================================================
// MarbleDB vs RocksDB Standalone Benchmark
//
// A self-contained benchmark comparing MarbleDB and RocksDB performance.
// No test framework dependencies - just raw performance comparison.
//==============================================================================

#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <string>
#include <memory>
#include <filesystem>
#include <iomanip>

// MarbleDB includes
#include "marble/lsm_storage.h"
#include "marble/file_system.h"

// RocksDB includes
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

namespace fs = std::filesystem;

//==============================================================================
// Benchmark Configuration
//==============================================================================

struct BenchmarkConfig {
    size_t num_keys = 100000;
    size_t value_size = 100;
    size_t batch_size = 1000;
    bool sync_writes = false;
    std::string marble_path = "/tmp/marble_bench";
    std::string rocks_path = "/tmp/rocks_bench";
};

//==============================================================================
// Benchmark Results
//==============================================================================

struct BenchmarkResults {
    std::string engine;
    double write_ops_per_sec = 0;
    double read_ops_per_sec = 0;
    double scan_ops_per_sec = 0;
    double write_latency_us = 0;
    double read_latency_us = 0;
    size_t data_size_bytes = 0;

    void Print() const {
        std::cout << "\n+------------------------------------------------------+" << std::endl;
        std::cout << "|  " << std::left << std::setw(52) << engine << " |" << std::endl;
        std::cout << "+------------------------------------------------------+" << std::endl;
        std::cout << "|  Write throughput:  " << std::right << std::setw(12) << std::fixed << std::setprecision(0) << write_ops_per_sec << " ops/sec      |" << std::endl;
        std::cout << "|  Read throughput:   " << std::right << std::setw(12) << read_ops_per_sec << " ops/sec      |" << std::endl;
        std::cout << "|  Scan throughput:   " << std::right << std::setw(12) << scan_ops_per_sec << " ops/sec      |" << std::endl;
        std::cout << "|  Write latency:     " << std::right << std::setw(12) << std::setprecision(2) << write_latency_us << " us           |" << std::endl;
        std::cout << "|  Read latency:      " << std::right << std::setw(12) << read_latency_us << " us           |" << std::endl;
        std::cout << "+------------------------------------------------------+" << std::endl;
    }
};

//==============================================================================
// Utility Functions
//==============================================================================

void CleanupDirectory(const std::string& path) {
    if (fs::exists(path)) {
        fs::remove_all(path);
    }
    fs::create_directories(path);
}

std::string GenerateRandomValue(size_t size, std::mt19937& gen) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);
    std::string result(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        result[i] = charset[dist(gen)];
    }
    return result;
}

//==============================================================================
// MarbleDB Benchmark
//==============================================================================

BenchmarkResults BenchmarkMarbleDB(const BenchmarkConfig& config) {
    BenchmarkResults results;
    results.engine = "MarbleDB (Arrow-native LSM)";

    CleanupDirectory(config.marble_path);

    // Create MarbleDB LSM tree
    marble::LSMTreeConfig lsm_config;
    lsm_config.data_directory = config.marble_path + "/data";
    lsm_config.wal_directory = config.marble_path + "/wal";
    lsm_config.temp_directory = config.marble_path + "/temp";
    lsm_config.memtable_max_size_bytes = 4 * 1024 * 1024;  // 4MB memtable
    lsm_config.l0_compaction_trigger = 4;
    lsm_config.max_levels = 7;

    // Create directories
    fs::create_directories(lsm_config.data_directory);
    fs::create_directories(lsm_config.wal_directory);
    fs::create_directories(lsm_config.temp_directory);

    auto lsm = marble::CreateLSMTree();
    auto status = lsm->Init(lsm_config);
    if (!status.ok()) {
        std::cerr << "Failed to create MarbleDB: " << status.ToString() << std::endl;
        return results;
    }

    std::mt19937 gen(42);  // Fixed seed for reproducibility

    //--------------------------------------------------------------------------
    // Write benchmark
    //--------------------------------------------------------------------------
    {
        auto start = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < config.num_keys; ++i) {
            uint64_t key = i;
            std::string value = GenerateRandomValue(config.value_size, gen);
            status = lsm->Put(key, value);
            if (!status.ok()) {
                std::cerr << "MarbleDB Put failed: " << status.ToString() << std::endl;
                return results;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.write_ops_per_sec = (config.num_keys * 1000000.0) / duration.count();
        results.write_latency_us = static_cast<double>(duration.count()) / config.num_keys;
    }

    //--------------------------------------------------------------------------
    // Read benchmark (point lookups)
    //--------------------------------------------------------------------------
    {
        auto start = std::chrono::high_resolution_clock::now();

        std::string value;
        for (size_t i = 0; i < config.num_keys; ++i) {
            uint64_t key = i;
            status = lsm->Get(key, &value);
            if (!status.ok() && !status.IsNotFound()) {
                std::cerr << "MarbleDB Get failed: " << status.ToString() << std::endl;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.read_ops_per_sec = (config.num_keys * 1000000.0) / duration.count();
        results.read_latency_us = static_cast<double>(duration.count()) / config.num_keys;
    }

    //--------------------------------------------------------------------------
    // Scan benchmark (using Scan API)
    //--------------------------------------------------------------------------
    {
        auto start = std::chrono::high_resolution_clock::now();

        std::vector<std::pair<uint64_t, std::string>> scan_results;
        status = lsm->Scan(0, config.num_keys - 1, &scan_results);
        size_t count = scan_results.size();

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.scan_ops_per_sec = (count * 1000000.0) / duration.count();
    }

    lsm->Shutdown();

    return results;
}

//==============================================================================
// RocksDB Benchmark
//==============================================================================

BenchmarkResults BenchmarkRocksDB(const BenchmarkConfig& config) {
    BenchmarkResults results;
    results.engine = "RocksDB (baseline)";

    CleanupDirectory(config.rocks_path);

    // Create RocksDB
    rocksdb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 4 * 1024 * 1024;  // 4MB memtable (match MarbleDB)
    options.max_write_buffer_number = 3;
    options.level0_file_num_compaction_trigger = 4;
    options.num_levels = 7;

    rocksdb::DB* db_raw;
    rocksdb::Status rocks_status = rocksdb::DB::Open(options, config.rocks_path, &db_raw);
    if (!rocks_status.ok()) {
        std::cerr << "Failed to create RocksDB: " << rocks_status.ToString() << std::endl;
        return results;
    }
    std::unique_ptr<rocksdb::DB> db(db_raw);

    std::mt19937 gen(42);  // Same seed as MarbleDB

    rocksdb::WriteOptions write_opts;
    write_opts.sync = config.sync_writes;

    //--------------------------------------------------------------------------
    // Write benchmark
    //--------------------------------------------------------------------------
    {
        auto start = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < config.num_keys; ++i) {
            // Use same key format as MarbleDB (numeric keys stored as string)
            std::string key = std::to_string(i);
            std::string value = GenerateRandomValue(config.value_size, gen);
            rocks_status = db->Put(write_opts, key, value);
            if (!rocks_status.ok()) {
                std::cerr << "RocksDB Put failed: " << rocks_status.ToString() << std::endl;
                return results;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.write_ops_per_sec = (config.num_keys * 1000000.0) / duration.count();
        results.write_latency_us = static_cast<double>(duration.count()) / config.num_keys;
    }

    //--------------------------------------------------------------------------
    // Read benchmark
    //--------------------------------------------------------------------------
    {
        auto start = std::chrono::high_resolution_clock::now();

        rocksdb::ReadOptions read_opts;
        std::string value;
        for (size_t i = 0; i < config.num_keys; ++i) {
            std::string key = std::to_string(i);
            rocks_status = db->Get(read_opts, key, &value);
            if (!rocks_status.ok() && !rocks_status.IsNotFound()) {
                std::cerr << "RocksDB Get failed: " << rocks_status.ToString() << std::endl;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.read_ops_per_sec = (config.num_keys * 1000000.0) / duration.count();
        results.read_latency_us = static_cast<double>(duration.count()) / config.num_keys;
    }

    //--------------------------------------------------------------------------
    // Scan benchmark
    //--------------------------------------------------------------------------
    {
        auto start = std::chrono::high_resolution_clock::now();

        size_t count = 0;
        rocksdb::ReadOptions read_opts;
        std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(read_opts));
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            auto k = iter->key();
            auto v = iter->value();
            (void)k;
            (void)v;
            count++;
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.scan_ops_per_sec = (count * 1000000.0) / duration.count();
    }

    return results;
}

//==============================================================================
// Main
//==============================================================================

int main(int argc, char** argv) {
    BenchmarkConfig config;

    // Parse command line args
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--keys" && i + 1 < argc) {
            config.num_keys = std::stoull(argv[++i]);
        } else if (arg == "--value-size" && i + 1 < argc) {
            config.value_size = std::stoull(argv[++i]);
        } else if (arg == "--sync") {
            config.sync_writes = true;
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << "  --keys N         Number of keys (default: 100000)" << std::endl;
            std::cout << "  --value-size N   Value size in bytes (default: 100)" << std::endl;
            std::cout << "  --sync           Enable sync writes" << std::endl;
            return 0;
        }
    }

    std::cout << "\n";
    std::cout << "+================================================================+" << std::endl;
    std::cout << "|        MarbleDB vs RocksDB Performance Benchmark               |" << std::endl;
    std::cout << "+================================================================+" << std::endl;
    std::cout << "|  Keys:        " << std::setw(10) << config.num_keys << "                                   |" << std::endl;
    std::cout << "|  Value size:  " << std::setw(10) << config.value_size << " bytes                            |" << std::endl;
    std::cout << "|  Sync writes: " << std::setw(10) << (config.sync_writes ? "yes" : "no") << "                                   |" << std::endl;
    std::cout << "+================================================================+" << std::endl;

    // Run benchmarks
    std::cout << "\nRunning MarbleDB benchmark..." << std::flush;
    auto marble_results = BenchmarkMarbleDB(config);
    std::cout << " done." << std::endl;

    std::cout << "Running RocksDB benchmark..." << std::flush;
    auto rocks_results = BenchmarkRocksDB(config);
    std::cout << " done." << std::endl;

    // Print results
    marble_results.Print();
    rocks_results.Print();

    // Print comparison
    std::cout << "\n+------------------------------------------------------+" << std::endl;
    std::cout << "|                    Comparison                        |" << std::endl;
    std::cout << "+------------------------------------------------------+" << std::endl;

    double write_ratio = marble_results.write_ops_per_sec / rocks_results.write_ops_per_sec;
    double read_ratio = marble_results.read_ops_per_sec / rocks_results.read_ops_per_sec;
    double scan_ratio = marble_results.scan_ops_per_sec / rocks_results.scan_ops_per_sec;

    auto format_ratio = [](double ratio) -> std::string {
        char buf[64];
        if (ratio >= 1.0) {
            snprintf(buf, sizeof(buf), "%.2fx faster", ratio);
        } else {
            snprintf(buf, sizeof(buf), "%.2fx slower", 1.0/ratio);
        }
        return std::string(buf);
    };

    std::cout << "|  Write: MarbleDB is " << std::left << std::setw(30) << format_ratio(write_ratio) << " |" << std::endl;
    std::cout << "|  Read:  MarbleDB is " << std::left << std::setw(30) << format_ratio(read_ratio) << " |" << std::endl;
    std::cout << "|  Scan:  MarbleDB is " << std::left << std::setw(30) << format_ratio(scan_ratio) << " |" << std::endl;
    std::cout << "+------------------------------------------------------+" << std::endl;

    // Cleanup
    CleanupDirectory(config.marble_path);
    CleanupDirectory(config.rocks_path);

    return 0;
}
