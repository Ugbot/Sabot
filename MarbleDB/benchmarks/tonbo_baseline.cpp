/**
 * @file tonbo_baseline.cpp
 * @brief Tonbo performance baseline for comparison with MarbleDB and RocksDB
 *
 * This benchmark establishes Tonbo performance baselines using the same
 * workload as RocksDB and MarbleDB benchmarks.
 *
 * Build:
 *   cd /Users/bengamble/Sabot/MarbleDB/build
 *   cmake ..
 *   make tonbo_baseline
 *
 * Run:
 *   ./tonbo_baseline
 */

#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <random>
#include <algorithm>
#include <sstream>
#include <cstring>

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
// Tonbo Benchmark
//==============================================================================

class TonboBenchmark {
public:
    TonboBenchmark(const std::string& db_path) : db_path_(db_path), db_(nullptr) {}

    ~TonboBenchmark() {
        Close();
    }

    bool Open() {
        CleanDirectory(db_path_);

        db_ = tonbo_db_open(db_path_.c_str());
        if (!db_) {
            std::cerr << "Failed to open Tonbo database" << std::endl;
            return false;
        }

        return true;
    }

    bool Reopen() {
        Close();

        db_ = tonbo_db_open(db_path_.c_str());
        if (!db_) {
            std::cerr << "Failed to reopen Tonbo database" << std::endl;
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

    BenchmarkResult RunSequentialWrites(size_t num_keys, size_t value_size = 512) {
        BenchmarkResult result;
        result.name = "Sequential Writes (" + std::to_string(num_keys) + " keys)";
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

        // Tonbo scan: NULL start/end means full scan
        TonboIter* iter = tonbo_db_scan(db_, nullptr, nullptr, -1);
        if (!iter) {
            std::cerr << "Failed to create iterator" << std::endl;
            result.duration_ms = 0;
            result.operations = 0;
            result.ops_per_sec = 0;
            result.latency_us = 0;
            return result;
        }

        char* key_ptr = nullptr;
        uint8_t* value_ptr = nullptr;
        uintptr_t value_len = 0;

        while (tonbo_iter_next(iter, &key_ptr, &value_ptr, &value_len) == 0) {
            count++;
            // Free key and value if needed
            if (key_ptr) {
                free(key_ptr);
                key_ptr = nullptr;
            }
            if (value_ptr) {
                tonbo_free_bytes(value_ptr, value_len);
                value_ptr = nullptr;
            }
        }

        tonbo_iter_free(iter);

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

private:
    std::string db_path_;
    TonboDb* db_;
};

//==============================================================================
// Main
//==============================================================================

int main() {
    std::cout << "=====================================================================" << std::endl;
    std::cout << "Tonbo Performance Baseline" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    // Configuration
    const size_t NUM_KEYS = 100000;
    const size_t NUM_LOOKUPS = 10000;
    const std::string DB_PATH = "/tmp/tonbo_baseline";

    std::cout << "Configuration:" << std::endl;
    std::cout << "  Dataset size: " << NUM_KEYS << " keys" << std::endl;
    std::cout << "  Lookup queries: " << NUM_LOOKUPS << std::endl;
    std::cout << "  Value size: 512 bytes" << std::endl;
    std::cout << "  Tonbo LSM-tree (Rust implementation)" << std::endl;
    std::cout << std::endl;

    TonboBenchmark tonbo(DB_PATH);

    std::cout << "Opening database..." << std::endl;
    if (!tonbo.Open()) {
        std::cerr << "Failed to open Tonbo database" << std::endl;
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

    auto write_result = tonbo.RunSequentialWrites(NUM_KEYS, 512);
    write_result.Print();

    std::cout << "└─────────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // 2. POINT LOOKUP PERFORMANCE
    //==========================================================================

    std::cout << "┌─────────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ 2. Point Lookup Performance                                             │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;
    std::cout << "│ Benchmark                                   │ Throughput  │ Latency     │" << std::endl;
    std::cout << "├─────────────────────────────────────────────────────────────────────────┤" << std::endl;

    auto lookup_result = tonbo.RunPointLookups(NUM_LOOKUPS, NUM_KEYS);
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

    auto scan_result = tonbo.RunRangeScan(NUM_KEYS);
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

    auto restart_result = tonbo.RunRestartBenchmark();
    restart_result.Print();

    std::cout << "└─────────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    //==========================================================================
    // SUMMARY
    //==========================================================================

    std::cout << "=====================================================================" << std::endl;
    std::cout << "Summary - Tonbo Baseline Results" << std::endl;
    std::cout << "=====================================================================" << std::endl;
    std::cout << std::endl;

    std::cout << "Tonbo is a Rust-based LSM-tree database with Arrow & Parquet storage." << std::endl;
    std::cout << "These results can be compared with MarbleDB and RocksDB." << std::endl;
    std::cout << std::endl;

    std::cout << "Tonbo Results:" << std::endl;
    std::cout << "  Write throughput:    " << std::fixed << std::setprecision(2)
             << (write_result.ops_per_sec / 1000.0) << " K ops/sec" << std::endl;
    std::cout << "  Point lookup:        " << std::fixed << std::setprecision(3)
             << lookup_result.latency_us << " μs/op" << std::endl;
    std::cout << "  Range scan:          " << std::fixed << std::setprecision(2)
             << (scan_result.ops_per_sec / 1000000.0) << " M rows/sec" << std::endl;
    std::cout << "  Database restart:    " << std::fixed << std::setprecision(2)
             << restart_result.latency_us / 1000.0 << " ms" << std::endl;
    std::cout << std::endl;

    tonbo.Close();

    std::cout << "Benchmark complete!" << std::endl;

    return 0;
}
