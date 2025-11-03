#pragma once

#include <marble/api.h>
#include <marble/db.h>
#include <marble/status.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <string>
#include <vector>
#include <memory>
#include <map>
#include <chrono>
#include <random>

namespace marble {
namespace bench {

// ============================================================================
// Configuration Structures
// ============================================================================

struct BenchmarkConfig {
    // Database configuration
    std::string db_path = "/tmp/marble_bench";
    bool clean_on_start = true;
    bool enable_wal = true;
    size_t memtable_size_mb = 64;

    // Benchmark selection
    std::vector<std::string> benchmarks;  // "crud", "batch", "analytics", "all"

    // Dataset sizes
    std::map<std::string, size_t> dataset_sizes = {
        {"tiny", 100},
        {"small", 1000},
        {"medium", 10000},
        {"large", 100000},
        {"xlarge", 1000000}
    };
    std::string default_size = "medium";

    // Batch operation settings
    std::vector<size_t> batch_sizes = {100, 1000, 10000};

    // Concurrency settings
    size_t num_threads = 1;

    // Output settings
    std::vector<std::string> output_formats;  // "console", "json"
    std::string json_output_file;
    bool verbose = false;

    // Randomization
    bool randomize_data = true;
    uint32_t random_seed = 0;  // 0 = use random_device

    // Performance targets (for comparison)
    struct Targets {
        double put_ops_sec = 400000.0;
        double get_ops_sec = 700000.0;
        double batch_rows_sec = 1000000.0;
        double scan_rows_sec = 5000000.0;
    } targets;
};

// ============================================================================
// Results Structures
// ============================================================================

struct LatencyStats {
    double avg_ms = 0.0;
    double min_ms = 0.0;
    double max_ms = 0.0;
    double p50_ms = 0.0;
    double p95_ms = 0.0;
    double p99_ms = 0.0;
};

struct BenchmarkResult {
    std::string name;
    std::string category;  // "crud", "batch", "analytics"

    // Throughput metrics
    double operations_per_sec = 0.0;
    double rows_per_sec = 0.0;
    double mb_per_sec = 0.0;

    // Latency metrics
    LatencyStats latency;

    // Additional metrics
    size_t total_operations = 0;
    size_t total_rows = 0;
    size_t total_bytes = 0;
    double duration_seconds = 0.0;

    // Memory metrics
    size_t memory_used_mb = 0;

    // Success/failure
    bool success = false;
    std::string error_message;

    // Performance comparison
    double vs_target_percent = 0.0;  // % of target performance achieved
};

struct BenchmarkSuite {
    std::string timestamp;
    BenchmarkConfig config;
    std::vector<BenchmarkResult> results;

    // Summary statistics
    size_t total_tests = 0;
    size_t passed_tests = 0;
    size_t failed_tests = 0;
    double total_duration_seconds = 0.0;
};

// ============================================================================
// Test Harness Class
// ============================================================================

class MarbleBenchHarness {
public:
    MarbleBenchHarness() = default;
    ~MarbleBenchHarness() = default;

    // Configuration
    Status LoadConfigFromFile(const std::string& config_file);
    Status LoadConfigFromArgs(int argc, char** argv);
    void SetConfig(const BenchmarkConfig& config);

    // Lifecycle
    Status Initialize();
    Status RunBenchmarks();
    Status Cleanup();

    // Results access
    const BenchmarkSuite& GetResults() const { return suite_; }

    // Output
    void PrintResults() const;
    Status ExportJSON(const std::string& filename) const;

private:
    // ========================================================================
    // Benchmark Category Runners
    // ========================================================================

    Status RunCRUDBenchmarks();
    Status RunBatchBenchmarks();
    Status RunAnalyticsBenchmarks();

    // ========================================================================
    // Individual CRUD Benchmarks
    // ========================================================================

    BenchmarkResult BenchmarkSequentialPut(size_t num_ops);
    BenchmarkResult BenchmarkRandomGet(size_t num_ops);
    BenchmarkResult BenchmarkPointDelete(size_t num_ops);
    BenchmarkResult BenchmarkMultiGet(size_t num_ops, size_t batch_size);

    // ========================================================================
    // Individual Batch Benchmarks
    // ========================================================================

    BenchmarkResult BenchmarkInsertBatch(size_t num_batches, size_t batch_size);
    BenchmarkResult BenchmarkScanTable(size_t expected_rows);
    BenchmarkResult BenchmarkFilteredScan(size_t expected_rows, double selectivity);

    // ========================================================================
    // Individual Analytics Benchmarks
    // ========================================================================

    BenchmarkResult BenchmarkSequentialScan(size_t num_rows);
    BenchmarkResult BenchmarkAggregation(size_t num_rows, const std::string& agg_type);
    BenchmarkResult BenchmarkZoneMapFiltering(size_t num_rows);

    // ========================================================================
    // Data Generation
    // ========================================================================

    std::shared_ptr<arrow::RecordBatch> GenerateRandomBatch(
        size_t num_rows,
        const std::string& schema_type = "default");

    std::shared_ptr<Record> GenerateRandomRecord(int64_t id);
    std::vector<Key> GenerateRandomKeys(size_t count);

    // ========================================================================
    // Utility Functions
    // ========================================================================

    Status OpenOrCreateDatabase();
    Status CloseDatabase();
    Status CreateBenchmarkTable(const std::string& table_name);

    void RecordResult(const BenchmarkResult& result);
    void CalculateSummaryStats();

    std::string FormatThroughput(double ops_per_sec) const;
    std::string FormatLatency(double ms) const;
    std::string FormatBytes(size_t bytes) const;

    // JSON export helpers
    std::string ResultToJSON(const BenchmarkResult& result) const;
    std::string SuiteToJSON() const;

    // Console output helpers
    void PrintHeader(const std::string& category) const;
    void PrintResult(const BenchmarkResult& result) const;
    void PrintSummary() const;

    // ========================================================================
    // Member Variables
    // ========================================================================

    BenchmarkConfig config_;
    BenchmarkSuite suite_;
    std::unique_ptr<MarbleDB> db_;

    // Random number generation
    std::mt19937 rng_;
    std::uniform_int_distribution<int64_t> id_dist_;
    std::uniform_real_distribution<double> value_dist_;
    std::uniform_int_distribution<int> string_length_dist_;

    // Timing
    std::chrono::high_resolution_clock::time_point suite_start_time_;
};

// ============================================================================
// Helper Functions
// ============================================================================

// Parse command-line arguments
Status ParseCommandLineArgs(int argc, char** argv, BenchmarkConfig* config);

// Parse JSON config file
Status ParseJSONConfig(const std::string& filename, BenchmarkConfig* config);

// Get current timestamp as ISO 8601 string
std::string GetISO8601Timestamp();

// Timer utility
class Timer {
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}

    double elapsed_seconds() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double>(end - start_).count();
    }

    double elapsed_milliseconds() const {
        return elapsed_seconds() * 1000.0;
    }

    double elapsed_microseconds() const {
        return elapsed_seconds() * 1000000.0;
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

} // namespace bench
} // namespace marble
