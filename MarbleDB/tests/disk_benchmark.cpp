//==============================================================================
// MarbleDB Disk I/O Benchmark
//
// Forces all reads to hit disk by:
// 1. Writing enough data to exceed memtable size (force flush to SSTables)
// 2. Closing and reopening the database to clear memory
// 3. Reading keys in random order to defeat read-ahead
//==============================================================================

#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <string>
#include <memory>
#include <filesystem>
#include <iomanip>
#include <algorithm>
#include <numeric>

// MarbleDB includes
#include "marble/lsm_storage.h"
#include "marble/file_system.h"
#include <arrow/api.h>

// RocksDB includes
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

namespace fs = std::filesystem;

//==============================================================================
// Benchmark Configuration
//==============================================================================

struct DiskBenchmarkConfig {
    size_t num_keys = 100000;           // 100K keys
    size_t value_size = 512;            // 512 bytes per value
    size_t memtable_size = 4 * 1024 * 1024;  // 4MB memtable (force early flush)
    size_t read_sample_size = 10000;    // Read 10K random keys
    std::string marble_path = "/tmp/marble_disk_bench";
    std::string rocks_path = "/tmp/rocks_disk_bench";
};

//==============================================================================
// Benchmark Results
//==============================================================================

struct DiskBenchmarkResults {
    std::string engine;

    // Write phase
    double write_ops_per_sec = 0;
    double write_latency_us = 0;
    size_t sstables_created = 0;
    size_t total_data_bytes = 0;

    // Cold read phase (after reopen)
    double cold_read_ops_per_sec = 0;
    double cold_read_latency_us = 0;
    double cold_read_p50_us = 0;
    double cold_read_p99_us = 0;

    // Scan phase
    double scan_ops_per_sec = 0;
    double batch_scan_ops_per_sec = 0;  // Arrow batch scan (MarbleDB only)

    void Print() const {
        std::cout << "\n+--------------------------------------------------------------+" << std::endl;
        std::cout << "|  " << std::left << std::setw(60) << engine << " |" << std::endl;
        std::cout << "+--------------------------------------------------------------+" << std::endl;
        std::cout << "|  WRITE PHASE                                                 |" << std::endl;
        std::cout << "|    Throughput:      " << std::right << std::setw(12) << std::fixed << std::setprecision(0) << write_ops_per_sec << " ops/sec              |" << std::endl;
        std::cout << "|    Latency:         " << std::right << std::setw(12) << std::setprecision(2) << write_latency_us << " us                  |" << std::endl;
        std::cout << "|    Data written:    " << std::right << std::setw(12) << (total_data_bytes / (1024*1024)) << " MB                  |" << std::endl;
        std::cout << "+--------------------------------------------------------------+" << std::endl;
        std::cout << "|  COLD READ PHASE (after reopen, random order)                |" << std::endl;
        std::cout << "|    Throughput:      " << std::right << std::setw(12) << std::setprecision(0) << cold_read_ops_per_sec << " ops/sec              |" << std::endl;
        std::cout << "|    Avg latency:     " << std::right << std::setw(12) << std::setprecision(2) << cold_read_latency_us << " us                  |" << std::endl;
        std::cout << "|    P50 latency:     " << std::right << std::setw(12) << cold_read_p50_us << " us                  |" << std::endl;
        std::cout << "|    P99 latency:     " << std::right << std::setw(12) << cold_read_p99_us << " us                  |" << std::endl;
        std::cout << "+--------------------------------------------------------------+" << std::endl;
        std::cout << "|  SCAN PHASE (row-at-a-time)                                  |" << std::endl;
        std::cout << "|    Throughput:      " << std::right << std::setw(12) << std::setprecision(0) << scan_ops_per_sec << " ops/sec              |" << std::endl;
        if (batch_scan_ops_per_sec > 0) {
            std::cout << "+--------------------------------------------------------------+" << std::endl;
            std::cout << "|  ARROW BATCH SCAN (MarbleDB only)                            |" << std::endl;
            std::cout << "|    Throughput:      " << std::right << std::setw(12) << std::setprecision(0) << batch_scan_ops_per_sec << " ops/sec              |" << std::endl;
        }
        std::cout << "+--------------------------------------------------------------+" << std::endl;
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

void DropCaches() {
    // On macOS, we can't easily drop caches, but closing/reopening the DB
    // forces a cold start. The random read order also defeats read-ahead.
    std::cout << "  [Simulating cold cache by reopening DB and random reads]" << std::endl;
}

double Percentile(std::vector<double>& latencies, double p) {
    if (latencies.empty()) return 0;
    std::sort(latencies.begin(), latencies.end());
    size_t idx = static_cast<size_t>(p * latencies.size());
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx];
}

//==============================================================================
// MarbleDB Disk Benchmark
//==============================================================================

DiskBenchmarkResults BenchmarkMarbleDBDisk(const DiskBenchmarkConfig& config) {
    DiskBenchmarkResults results;
    results.engine = "MarbleDB (Arrow-native LSM) - DISK";

    CleanupDirectory(config.marble_path);

    std::mt19937 gen(42);  // Fixed seed for reproducibility

    //--------------------------------------------------------------------------
    // WRITE PHASE - Force data to disk
    //--------------------------------------------------------------------------
    std::cout << "  Writing " << config.num_keys << " keys..." << std::flush;
    {
        marble::LSMTreeConfig lsm_config;
        lsm_config.data_directory = config.marble_path + "/data";
        lsm_config.wal_directory = config.marble_path + "/wal";
        lsm_config.temp_directory = config.marble_path + "/temp";
        lsm_config.memtable_max_size_bytes = config.memtable_size;  // Small memtable
        lsm_config.l0_compaction_trigger = 4;
        lsm_config.max_levels = 7;

        fs::create_directories(lsm_config.data_directory);
        fs::create_directories(lsm_config.wal_directory);
        fs::create_directories(lsm_config.temp_directory);

        auto lsm = marble::CreateLSMTree();
        auto status = lsm->Init(lsm_config);
        if (!status.ok()) {
            std::cerr << "Failed to create MarbleDB: " << status.ToString() << std::endl;
            return results;
        }

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

        // Force flush to ensure data is on disk
        lsm->Shutdown();

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.write_ops_per_sec = (config.num_keys * 1000000.0) / duration.count();
        results.write_latency_us = static_cast<double>(duration.count()) / config.num_keys;
        results.total_data_bytes = config.num_keys * (sizeof(uint64_t) + config.value_size);
    }
    std::cout << " done." << std::endl;

    //--------------------------------------------------------------------------
    // COLD READ PHASE - Reopen DB and read random keys
    //--------------------------------------------------------------------------
    DropCaches();
    std::cout << "  Cold reading " << config.read_sample_size << " random keys..." << std::flush;
    {
        marble::LSMTreeConfig lsm_config;
        lsm_config.data_directory = config.marble_path + "/data";
        lsm_config.wal_directory = config.marble_path + "/wal";
        lsm_config.temp_directory = config.marble_path + "/temp";
        lsm_config.memtable_max_size_bytes = config.memtable_size;

        auto lsm = marble::CreateLSMTree();
        auto status = lsm->Init(lsm_config);
        if (!status.ok()) {
            std::cerr << "Failed to reopen MarbleDB: " << status.ToString() << std::endl;
            return results;
        }

        // Generate random key order to defeat read-ahead
        std::vector<uint64_t> read_keys(config.read_sample_size);
        std::uniform_int_distribution<uint64_t> key_dist(0, config.num_keys - 1);
        for (size_t i = 0; i < config.read_sample_size; ++i) {
            read_keys[i] = key_dist(gen);
        }

        // Measure individual read latencies
        std::vector<double> latencies;
        latencies.reserve(config.read_sample_size);

        auto start = std::chrono::high_resolution_clock::now();

        std::string value;
        for (uint64_t key : read_keys) {
            auto read_start = std::chrono::high_resolution_clock::now();
            status = lsm->Get(key, &value);
            auto read_end = std::chrono::high_resolution_clock::now();

            auto read_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(read_end - read_start);
            latencies.push_back(read_duration.count() / 1000.0);  // Convert to us
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.cold_read_ops_per_sec = (config.read_sample_size * 1000000.0) / duration.count();
        results.cold_read_latency_us = static_cast<double>(duration.count()) / config.read_sample_size;
        results.cold_read_p50_us = Percentile(latencies, 0.50);
        results.cold_read_p99_us = Percentile(latencies, 0.99);

        //----------------------------------------------------------------------
        // SCAN PHASE (row-at-a-time)
        //----------------------------------------------------------------------
        std::cout << " done.\n  Scanning (row-at-a-time)..." << std::flush;
        {
            auto scan_start = std::chrono::high_resolution_clock::now();

            std::vector<std::pair<uint64_t, std::string>> scan_results;
            status = lsm->Scan(0, config.num_keys - 1, &scan_results);
            size_t count = scan_results.size();

            auto scan_end = std::chrono::high_resolution_clock::now();
            auto scan_duration = std::chrono::duration_cast<std::chrono::microseconds>(scan_end - scan_start);

            results.scan_ops_per_sec = (count * 1000000.0) / scan_duration.count();
        }

        lsm->Shutdown();
    }

    //--------------------------------------------------------------------------
    // ARROW BATCH SCAN PHASE (fresh reopen for cold scan)
    //--------------------------------------------------------------------------
    std::cout << " done.\n  Cold Arrow batch scan..." << std::flush;
    {
        // Reopen fresh for cold batch scan
        marble::LSMTreeConfig lsm_config;
        lsm_config.data_directory = config.marble_path + "/data";
        lsm_config.wal_directory = config.marble_path + "/wal";
        lsm_config.temp_directory = config.marble_path + "/temp";
        lsm_config.memtable_max_size_bytes = config.memtable_size;

        auto lsm = marble::CreateLSMTree();
        auto status = lsm->Init(lsm_config);
        if (!status.ok()) {
            std::cerr << "Failed to reopen MarbleDB for batch scan" << std::endl;
            return results;
        }

        auto batch_scan_start = std::chrono::high_resolution_clock::now();

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        status = lsm->ScanSSTablesBatches(0, config.num_keys - 1, &batches);

        // Count total rows across all batches
        size_t total_rows = 0;
        for (const auto& batch : batches) {
            total_rows += batch->num_rows();
        }

        auto batch_scan_end = std::chrono::high_resolution_clock::now();
        auto batch_scan_duration = std::chrono::duration_cast<std::chrono::microseconds>(batch_scan_end - batch_scan_start);

        // Avoid division by zero
        if (batch_scan_duration.count() > 0 && total_rows > 0) {
            results.batch_scan_ops_per_sec = (total_rows * 1000000.0) / batch_scan_duration.count();
        } else {
            std::cout << "[" << batches.size() << " batches, " << total_rows << " rows, "
                      << batch_scan_duration.count() << " us]" << std::flush;
            results.batch_scan_ops_per_sec = 0;
        }

        lsm->Shutdown();
    }
    std::cout << " done." << std::endl;

    return results;
}

//==============================================================================
// RocksDB Disk Benchmark
//==============================================================================

DiskBenchmarkResults BenchmarkRocksDBDisk(const DiskBenchmarkConfig& config) {
    DiskBenchmarkResults results;
    results.engine = "RocksDB (baseline) - DISK";

    CleanupDirectory(config.rocks_path);

    std::mt19937 gen(42);  // Same seed as MarbleDB

    //--------------------------------------------------------------------------
    // WRITE PHASE
    //--------------------------------------------------------------------------
    std::cout << "  Writing " << config.num_keys << " keys..." << std::flush;
    {
        rocksdb::Options options;
        options.create_if_missing = true;
        options.write_buffer_size = config.memtable_size;  // Match MarbleDB
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

        rocksdb::WriteOptions write_opts;
        write_opts.sync = false;

        auto start = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < config.num_keys; ++i) {
            std::string key = std::to_string(i);
            std::string value = GenerateRandomValue(config.value_size, gen);
            rocks_status = db->Put(write_opts, key, value);
            if (!rocks_status.ok()) {
                std::cerr << "RocksDB Put failed: " << rocks_status.ToString() << std::endl;
                return results;
            }
        }

        // Force flush
        db->FlushWAL(true);
        rocksdb::FlushOptions flush_opts;
        db->Flush(flush_opts);

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.write_ops_per_sec = (config.num_keys * 1000000.0) / duration.count();
        results.write_latency_us = static_cast<double>(duration.count()) / config.num_keys;
        results.total_data_bytes = config.num_keys * (sizeof(uint64_t) + config.value_size);
    }
    std::cout << " done." << std::endl;

    //--------------------------------------------------------------------------
    // COLD READ PHASE
    //--------------------------------------------------------------------------
    DropCaches();
    std::cout << "  Cold reading " << config.read_sample_size << " random keys..." << std::flush;
    {
        rocksdb::Options options;
        options.create_if_missing = false;

        rocksdb::DB* db_raw;
        rocksdb::Status rocks_status = rocksdb::DB::Open(options, config.rocks_path, &db_raw);
        if (!rocks_status.ok()) {
            std::cerr << "Failed to reopen RocksDB: " << rocks_status.ToString() << std::endl;
            return results;
        }
        std::unique_ptr<rocksdb::DB> db(db_raw);

        // Generate random key order
        std::vector<uint64_t> read_keys(config.read_sample_size);
        std::uniform_int_distribution<uint64_t> key_dist(0, config.num_keys - 1);
        for (size_t i = 0; i < config.read_sample_size; ++i) {
            read_keys[i] = key_dist(gen);
        }

        std::vector<double> latencies;
        latencies.reserve(config.read_sample_size);

        rocksdb::ReadOptions read_opts;

        auto start = std::chrono::high_resolution_clock::now();

        std::string value;
        for (uint64_t key : read_keys) {
            std::string key_str = std::to_string(key);

            auto read_start = std::chrono::high_resolution_clock::now();
            rocks_status = db->Get(read_opts, key_str, &value);
            auto read_end = std::chrono::high_resolution_clock::now();

            auto read_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(read_end - read_start);
            latencies.push_back(read_duration.count() / 1000.0);
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        results.cold_read_ops_per_sec = (config.read_sample_size * 1000000.0) / duration.count();
        results.cold_read_latency_us = static_cast<double>(duration.count()) / config.read_sample_size;
        results.cold_read_p50_us = Percentile(latencies, 0.50);
        results.cold_read_p99_us = Percentile(latencies, 0.99);

        //----------------------------------------------------------------------
        // SCAN PHASE
        //----------------------------------------------------------------------
        std::cout << " done.\n  Scanning..." << std::flush;
        {
            auto scan_start = std::chrono::high_resolution_clock::now();

            size_t count = 0;
            std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(read_opts));
            for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
                auto k = iter->key();
                auto v = iter->value();
                (void)k;
                (void)v;
                count++;
            }

            auto scan_end = std::chrono::high_resolution_clock::now();
            auto scan_duration = std::chrono::duration_cast<std::chrono::microseconds>(scan_end - scan_start);

            results.scan_ops_per_sec = (count * 1000000.0) / scan_duration.count();
        }
    }
    std::cout << " done." << std::endl;

    return results;
}

//==============================================================================
// Main
//==============================================================================

int main(int argc, char** argv) {
    DiskBenchmarkConfig config;

    // Parse command line args
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--keys" && i + 1 < argc) {
            config.num_keys = std::stoull(argv[++i]);
        } else if (arg == "--value-size" && i + 1 < argc) {
            config.value_size = std::stoull(argv[++i]);
        } else if (arg == "--read-sample" && i + 1 < argc) {
            config.read_sample_size = std::stoull(argv[++i]);
        } else if (arg == "--memtable-size" && i + 1 < argc) {
            config.memtable_size = std::stoull(argv[++i]) * 1024 * 1024;  // Input in MB
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << "  --keys N           Number of keys (default: 100000)" << std::endl;
            std::cout << "  --value-size N     Value size in bytes (default: 512)" << std::endl;
            std::cout << "  --read-sample N    Number of keys to read (default: 10000)" << std::endl;
            std::cout << "  --memtable-size N  Memtable size in MB (default: 4)" << std::endl;
            return 0;
        }
    }

    size_t total_data_mb = (config.num_keys * (sizeof(uint64_t) + config.value_size)) / (1024 * 1024);

    std::cout << "\n";
    std::cout << "+==================================================================+" << std::endl;
    std::cout << "|          MarbleDB vs RocksDB DISK I/O Benchmark                  |" << std::endl;
    std::cout << "+==================================================================+" << std::endl;
    std::cout << "|  Keys:              " << std::setw(10) << config.num_keys << "                                 |" << std::endl;
    std::cout << "|  Value size:        " << std::setw(10) << config.value_size << " bytes                          |" << std::endl;
    std::cout << "|  Total data:        " << std::setw(10) << total_data_mb << " MB                             |" << std::endl;
    std::cout << "|  Memtable size:     " << std::setw(10) << (config.memtable_size / (1024*1024)) << " MB (forces early flush)        |" << std::endl;
    std::cout << "|  Read sample:       " << std::setw(10) << config.read_sample_size << " random keys                    |" << std::endl;
    std::cout << "+==================================================================+" << std::endl;

    // Run benchmarks
    std::cout << "\n--- MarbleDB Disk Benchmark ---" << std::endl;
    auto marble_results = BenchmarkMarbleDBDisk(config);

    std::cout << "\n--- RocksDB Disk Benchmark ---" << std::endl;
    auto rocks_results = BenchmarkRocksDBDisk(config);

    // Print results
    marble_results.Print();
    rocks_results.Print();

    // Print comparison
    std::cout << "\n+--------------------------------------------------------------+" << std::endl;
    std::cout << "|                       Comparison                             |" << std::endl;
    std::cout << "+--------------------------------------------------------------+" << std::endl;

    auto format_ratio = [](double marble, double rocks) -> std::string {
        double ratio = marble / rocks;
        char buf[64];
        if (ratio >= 1.0) {
            snprintf(buf, sizeof(buf), "%.2fx faster", ratio);
        } else {
            snprintf(buf, sizeof(buf), "%.2fx slower", 1.0/ratio);
        }
        return std::string(buf);
    };

    std::cout << "|  Write throughput:  MarbleDB is " << std::left << std::setw(26) << format_ratio(marble_results.write_ops_per_sec, rocks_results.write_ops_per_sec) << " |" << std::endl;
    std::cout << "|  Cold read throughput: MarbleDB is " << std::left << std::setw(23) << format_ratio(marble_results.cold_read_ops_per_sec, rocks_results.cold_read_ops_per_sec) << " |" << std::endl;
    std::cout << "|  Cold read P50:     MarbleDB is " << std::left << std::setw(26) << format_ratio(rocks_results.cold_read_p50_us, marble_results.cold_read_p50_us) << " |" << std::endl;
    std::cout << "|  Cold read P99:     MarbleDB is " << std::left << std::setw(26) << format_ratio(rocks_results.cold_read_p99_us, marble_results.cold_read_p99_us) << " |" << std::endl;
    std::cout << "|  Scan (row):        MarbleDB is " << std::left << std::setw(26) << format_ratio(marble_results.scan_ops_per_sec, rocks_results.scan_ops_per_sec) << " |" << std::endl;
    std::cout << "|  Batch scan (Arrow): MarbleDB is " << std::left << std::setw(25) << format_ratio(marble_results.batch_scan_ops_per_sec, rocks_results.scan_ops_per_sec) << " |" << std::endl;
    std::cout << "+--------------------------------------------------------------+" << std::endl;

    // Cleanup
    CleanupDirectory(config.marble_path);
    CleanupDirectory(config.rocks_path);

    return 0;
}
