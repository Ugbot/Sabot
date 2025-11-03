/**
 * MarbleDB Benchmark Common Infrastructure
 *
 * Modeled after RocksDB's DBTestBase and Tonbo's benchmark patterns
 * Provides shared utilities for creating benchmarks with Arrow RecordBatches
 */

#pragma once

#include "marble/api.h"
#include "marble/db.h"
#include "marble/status.h"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <chrono>
#include <random>
#include <memory>
#include <vector>
#include <string>
#include <iostream>
#include <iomanip>

namespace marble {
namespace bench {

// Timer utility for performance measurement
class Timer {
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}

    void Reset() {
        start_ = std::chrono::high_resolution_clock::now();
    }

    double ElapsedSeconds() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double>(end - start_).count();
    }

    double ElapsedMillis() const {
        return ElapsedSeconds() * 1000.0;
    }

    uint64_t ElapsedMicros() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count();
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

// Benchmark result metrics
struct BenchmarkMetrics {
    std::string name;
    size_t operations{0};
    size_t total_rows{0};
    double duration_seconds{0.0};
    double ops_per_sec{0.0};
    double rows_per_sec{0.0};
    double avg_latency_us{0.0};
    bool success{false};

    void Print() const {
        std::cout << "\n=== " << name << " ===" << std::endl;
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "  Operations: " << operations << std::endl;
        std::cout << "  Total Rows: " << total_rows << std::endl;
        std::cout << "  Duration:   " << duration_seconds << " seconds" << std::endl;
        std::cout << "  Throughput: " << std::setprecision(0) << ops_per_sec << " ops/sec, "
                  << rows_per_sec << " rows/sec" << std::endl;
        std::cout << "  Latency:    " << std::setprecision(2) << avg_latency_us << " μs/op" << std::endl;
        std::cout << "  Status:     " << (success ? "✓ PASS" : "✗ FAIL") << std::endl;
    }
};

// Random data generator (with seed for reproducibility, like Tonbo)
class RandomDataGenerator {
public:
    explicit RandomDataGenerator(uint64_t seed = 12345)
        : rng_(seed)
        , id_dist_(0, 1000000000)
        , value_dist_(0.0, 1000000.0) {}

    int64_t NextId() {
        return id_dist_(rng_);
    }

    double NextValue() {
        return value_dist_(rng_);
    }

    std::string NextString(size_t length = 32) {
        static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        std::string result;
        result.reserve(length);
        std::uniform_int_distribution<> char_dist(0, sizeof(alphanum) - 2);
        for (size_t i = 0; i < length; ++i) {
            result += alphanum[char_dist(rng_)];
        }
        return result;
    }

private:
    std::mt19937_64 rng_;
    std::uniform_int_distribution<int64_t> id_dist_;
    std::uniform_real_distribution<double> value_dist_;
};

// Arrow RecordBatch builder utilities (inspired by Tonbo's batch creation)
class ArrowBatchBuilder {
public:
    // Create a simple test schema: (id: int64, timestamp: int64, value: double, category: string)
    static std::shared_ptr<arrow::Schema> GetTestSchema() {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("timestamp", arrow::int64()),
            arrow::field("value", arrow::float64()),
            arrow::field("category", arrow::utf8())
        });
    }

    // Create a RecordBatch with random test data
    static std::shared_ptr<arrow::RecordBatch> CreateTestBatch(
        size_t num_rows,
        RandomDataGenerator* rng = nullptr
    );

    // Create a RecordBatch with sequential IDs (for deterministic testing)
    static std::shared_ptr<arrow::RecordBatch> CreateSequentialBatch(
        int64_t start_id,
        size_t num_rows
    );
};

// Base class for MarbleDB benchmarks (modeled after RocksDB's DBTestBase)
class MarbleTestBase {
public:
    MarbleTestBase(const std::string& test_name, const std::string& db_path = "")
        : test_name_(test_name)
        , db_path_(db_path.empty() ? "/tmp/marble_bench_" + test_name : db_path)
        , rng_(12345) {}

    virtual ~MarbleTestBase() {
        Cleanup();
    }

    // Setup and teardown
    virtual Status Initialize(bool clean_on_start = true);
    virtual Status Cleanup();

    // Helper methods for common operations
    Status Put(const std::string& table_name, const std::string& key_str);
    Status InsertBatch(const std::string& table_name, std::shared_ptr<arrow::RecordBatch> batch);
    Status ScanTable(const std::string& table_name, std::unique_ptr<QueryResult>* result);

    // Create test table
    Status CreateTestTable(const std::string& table_name);

    // Accessors
    MarbleDB* db() { return db_.get(); }
    const std::string& db_path() const { return db_path_; }
    RandomDataGenerator* rng() { return &rng_; }

protected:
    std::string test_name_;
    std::string db_path_;
    std::unique_ptr<MarbleDB> db_;
    RandomDataGenerator rng_;
};

// Benchmark runner utility
class BenchmarkRunner {
public:
    static void RunAndReport(
        const std::string& name,
        std::function<Status()> benchmark_func,
        size_t operations,
        size_t total_rows = 0
    );
};

} // namespace bench
} // namespace marble
