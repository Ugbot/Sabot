#include "marble_bench_harness.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <ctime>
#include <cstring>
#include <sys/stat.h>

namespace marble {
namespace bench {

// ============================================================================
// Helper Functions Implementation
// ============================================================================

std::string GetISO8601Timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::gmtime(&time_t_now), "%Y-%m-%dT%H:%M:%SZ");
    return ss.str();
}

Status ParseCommandLineArgs(int argc, char** argv, BenchmarkConfig* config) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            std::cout << "MarbleDB Benchmark Harness\n\n";
            std::cout << "Usage: " << argv[0] << " [options]\n\n";
            std::cout << "Options:\n";
            std::cout << "  --config <file>       Load configuration from JSON file\n";
            std::cout << "  --benchmark <name>    Run specific benchmark (crud|batch|analytics|all)\n";
            std::cout << "  --size <size>         Dataset size (tiny|small|medium|large|xlarge)\n";
            std::cout << "  --batch-size <n>      Batch size for batch operations\n";
            std::cout << "  --threads <n>         Number of threads\n";
            std::cout << "  --output <format>     Output format (console|json|both)\n";
            std::cout << "  --json-file <file>    JSON output filename\n";
            std::cout << "  --db-path <path>      Database path\n";
            std::cout << "  --no-clean            Don't clean database on start\n";
            std::cout << "  --verbose             Verbose output\n";
            std::cout << "  --help, -h            Show this help\n";
            return Status::OK();
        }
        else if (arg == "--config" && i + 1 < argc) {
            // Config file will be loaded separately
            ++i;
        }
        else if (arg == "--benchmark" && i + 1 < argc) {
            config->benchmarks.push_back(argv[++i]);
        }
        else if (arg == "--size" && i + 1 < argc) {
            config->default_size = argv[++i];
        }
        else if (arg == "--batch-size" && i + 1 < argc) {
            size_t size = std::stoull(argv[++i]);
            config->batch_sizes = {size};
        }
        else if (arg == "--threads" && i + 1 < argc) {
            config->num_threads = std::stoull(argv[++i]);
        }
        else if (arg == "--output" && i + 1 < argc) {
            std::string fmt = argv[++i];
            if (fmt == "both") {
                config->output_formats = {"console", "json"};
            } else {
                config->output_formats = {fmt};
            }
        }
        else if (arg == "--json-file" && i + 1 < argc) {
            config->json_output_file = argv[++i];
        }
        else if (arg == "--db-path" && i + 1 < argc) {
            config->db_path = argv[++i];
        }
        else if (arg == "--no-clean") {
            config->clean_on_start = false;
        }
        else if (arg == "--verbose") {
            config->verbose = true;
        }
    }

    // Defaults
    if (config->benchmarks.empty()) {
        config->benchmarks = {"all"};
    }
    if (config->output_formats.empty()) {
        config->output_formats = {"console"};
    }

    return Status::OK();
}

Status ParseJSONConfig(const std::string& filename, BenchmarkConfig* config) {
    // Simple JSON parsing - for production, use nlohmann/json or similar
    std::ifstream file(filename);
    if (!file.is_open()) {
        return Status::IOError("Cannot open config file: " + filename);
    }

    // For now, return OK - would implement full JSON parsing here
    std::cout << "Note: JSON config file parsing not fully implemented yet\n";
    std::cout << "Using command-line defaults\n";
    return Status::OK();
}

// ============================================================================
// MarbleBenchHarness Implementation
// ============================================================================

Status MarbleBenchHarness::LoadConfigFromFile(const std::string& config_file) {
    return ParseJSONConfig(config_file, &config_);
}

Status MarbleBenchHarness::LoadConfigFromArgs(int argc, char** argv) {
    return ParseCommandLineArgs(argc, argv, &config_);
}

void MarbleBenchHarness::SetConfig(const BenchmarkConfig& config) {
    config_ = config;
}

Status MarbleBenchHarness::Initialize() {
    suite_start_time_ = std::chrono::high_resolution_clock::now();
    suite_.timestamp = GetISO8601Timestamp();
    suite_.config = config_;

    // Initialize random number generator
    if (config_.randomize_data) {
        if (config_.random_seed == 0) {
            std::random_device rd;
            rng_.seed(rd());
        } else {
            rng_.seed(config_.random_seed);
        }
    }

    // Setup distributions
    id_dist_ = std::uniform_int_distribution<int64_t>(1, 1000000000);
    value_dist_ = std::uniform_real_distribution<double>(0.0, 10000.0);
    string_length_dist_ = std::uniform_int_distribution<int>(10, 50);

    // Clean database if requested
    if (config_.clean_on_start) {
        std::string cmd = "rm -rf " + config_.db_path;
        system(cmd.c_str());
    }

    // Open database
    return OpenOrCreateDatabase();
}

Status MarbleBenchHarness::RunBenchmarks() {
    std::cout << "==========================================\n";
    std::cout << "MarbleDB Benchmark Harness\n";
    std::cout << "==========================================\n";
    std::cout << "Database: " << config_.db_path << "\n";
    std::cout << "Benchmarks: ";
    for (const auto& b : config_.benchmarks) std::cout << b << " ";
    std::cout << "\n==========================================\n\n";

    bool run_all = std::find(config_.benchmarks.begin(), config_.benchmarks.end(), "all")
                   != config_.benchmarks.end();

    if (run_all || std::find(config_.benchmarks.begin(), config_.benchmarks.end(), "crud")
                   != config_.benchmarks.end()) {
        auto status = RunCRUDBenchmarks();
        if (!status.ok()) {
            std::cerr << "CRUD benchmarks failed: " << status.message() << "\n";
        }
    }

    if (run_all || std::find(config_.benchmarks.begin(), config_.benchmarks.end(), "batch")
                   != config_.benchmarks.end()) {
        auto status = RunBatchBenchmarks();
        if (!status.ok()) {
            std::cerr << "Batch benchmarks failed: " << status.message() << "\n";
        }
    }

    if (run_all || std::find(config_.benchmarks.begin(), config_.benchmarks.end(), "analytics")
                   != config_.benchmarks.end()) {
        auto status = RunAnalyticsBenchmarks();
        if (!status.ok()) {
            std::cerr << "Analytics benchmarks failed: " << status.message() << "\n";
        }
    }

    CalculateSummaryStats();
    return Status::OK();
}

Status MarbleBenchHarness::Cleanup() {
    CloseDatabase();
    return Status::OK();
}

// ============================================================================
// Benchmark Category Runners
// ============================================================================

Status MarbleBenchHarness::RunCRUDBenchmarks() {
    PrintHeader("CRUD Benchmarks");

    size_t num_ops = config_.dataset_sizes[config_.default_size];

    // Sequential Put
    auto result = BenchmarkSequentialPut(num_ops);
    RecordResult(result);
    PrintResult(result);

    // Random Get
    result = BenchmarkRandomGet(num_ops / 2);  // Half of puts
    RecordResult(result);
    PrintResult(result);

    // Point Delete
    result = BenchmarkPointDelete(num_ops / 10);  // 10% of puts
    RecordResult(result);
    PrintResult(result);

    std::cout << "\n";
    return Status::OK();
}

Status MarbleBenchHarness::RunBatchBenchmarks() {
    PrintHeader("Batch Benchmarks");

    size_t base_rows = config_.dataset_sizes[config_.default_size];

    for (size_t batch_size : config_.batch_sizes) {
        size_t num_batches = base_rows / batch_size;
        if (num_batches == 0) num_batches = 1;

        // InsertBatch
        auto result = BenchmarkInsertBatch(num_batches, batch_size);
        RecordResult(result);
        PrintResult(result);

        // ScanTable
        result = BenchmarkScanTable(num_batches * batch_size);
        RecordResult(result);
        PrintResult(result);
    }

    std::cout << "\n";
    return Status::OK();
}

Status MarbleBenchHarness::RunAnalyticsBenchmarks() {
    PrintHeader("Analytics Benchmarks");

    size_t num_rows = config_.dataset_sizes[config_.default_size];

    // Sequential Scan
    auto result = BenchmarkSequentialScan(num_rows);
    RecordResult(result);
    PrintResult(result);

    // Aggregation
    result = BenchmarkAggregation(num_rows, "SUM");
    RecordResult(result);
    PrintResult(result);

    std::cout << "\n";
    return Status::OK();
}

// ============================================================================
// Individual CRUD Benchmarks
// ============================================================================

BenchmarkResult MarbleBenchHarness::BenchmarkSequentialPut(size_t num_ops) {
    BenchmarkResult result;
    result.name = "Sequential Put";
    result.category = "crud";
    result.total_operations = num_ops;

    Timer timer;
    size_t successful = 0;

    for (size_t i = 0; i < num_ops; ++i) {
        auto record = GenerateRandomRecord(i);
        WriteOptions options;
        auto status = db_->Put(options, record);
        if (status.ok()) {
            successful++;
        }
    }

    result.duration_seconds = timer.elapsed_seconds();
    result.operations_per_sec = successful / result.duration_seconds;
    result.success = (successful == num_ops);
    result.vs_target_percent = (result.operations_per_sec / config_.targets.put_ops_sec) * 100.0;

    return result;
}

BenchmarkResult MarbleBenchHarness::BenchmarkRandomGet(size_t num_ops) {
    BenchmarkResult result;
    result.name = "Random Get";
    result.category = "crud";
    result.total_operations = num_ops;

    Timer timer;
    size_t successful = 0;

    for (size_t i = 0; i < num_ops; ++i) {
        int64_t id = id_dist_(rng_) % num_ops;  // Random ID within range
        auto keys = GenerateRandomKeys(1);

        std::shared_ptr<Record> record;
        ReadOptions options;
        auto status = db_->Get(options, keys[0], &record);
        if (status.ok()) {
            successful++;
        }
    }

    result.duration_seconds = timer.elapsed_seconds();
    result.operations_per_sec = successful / result.duration_seconds;
    result.success = true;  // Gets can return not found
    result.vs_target_percent = (result.operations_per_sec / config_.targets.get_ops_sec) * 100.0;

    return result;
}

BenchmarkResult MarbleBenchHarness::BenchmarkPointDelete(size_t num_ops) {
    BenchmarkResult result;
    result.name = "Point Delete";
    result.category = "crud";
    result.total_operations = num_ops;

    Timer timer;
    size_t successful = 0;

    for (size_t i = 0; i < num_ops; ++i) {
        auto keys = GenerateRandomKeys(1);
        WriteOptions options;
        auto status = db_->Delete(options, keys[0]);
        if (status.ok()) {
            successful++;
        }
    }

    result.duration_seconds = timer.elapsed_seconds();
    result.operations_per_sec = successful / result.duration_seconds;
    result.success = (successful == num_ops);

    return result;
}

// ============================================================================
// Individual Batch Benchmarks
// ============================================================================

BenchmarkResult MarbleBenchHarness::BenchmarkInsertBatch(size_t num_batches, size_t batch_size) {
    BenchmarkResult result;
    result.name = "InsertBatch(" + std::to_string(batch_size) + ")";
    result.category = "batch";
    result.total_operations = num_batches;
    result.total_rows = num_batches * batch_size;

    Timer timer;
    size_t successful = 0;

    for (size_t i = 0; i < num_batches; ++i) {
        auto batch = GenerateRandomBatch(batch_size);
        auto status = db_->InsertBatch("benchmark", batch);
        if (status.ok()) {
            successful++;
        }
    }

    result.duration_seconds = timer.elapsed_seconds();
    result.operations_per_sec = successful / result.duration_seconds;
    result.rows_per_sec = result.total_rows / result.duration_seconds;
    result.success = (successful == num_batches);
    result.vs_target_percent = (result.rows_per_sec / config_.targets.batch_rows_sec) * 100.0;

    return result;
}

BenchmarkResult MarbleBenchHarness::BenchmarkScanTable(size_t expected_rows) {
    BenchmarkResult result;
    result.name = "ScanTable";
    result.category = "batch";
    result.total_operations = 1;

    Timer timer;

    std::unique_ptr<QueryResult> query_result;
    auto status = db_->ScanTable("benchmark", &query_result);

    result.duration_seconds = timer.elapsed_seconds();
    result.success = status.ok();

    if (query_result) {
        result.rows_per_sec = expected_rows / result.duration_seconds;
        result.total_rows = expected_rows;
    }

    result.vs_target_percent = (result.rows_per_sec / config_.targets.scan_rows_sec) * 100.0;

    return result;
}

BenchmarkResult MarbleBenchHarness::BenchmarkFilteredScan(size_t expected_rows, double selectivity) {
    BenchmarkResult result;
    result.name = "Filtered Scan (" + std::to_string((int)(selectivity * 100)) + "%)";
    result.category = "batch";

    // Placeholder - would implement filtered scan here
    result.success = false;
    result.error_message = "Not implemented";

    return result;
}

// ============================================================================
// Individual Analytics Benchmarks
// ============================================================================

BenchmarkResult MarbleBenchHarness::BenchmarkSequentialScan(size_t num_rows) {
    BenchmarkResult result;
    result.name = "Sequential Scan";
    result.category = "analytics";
    result.total_rows = num_rows;

    Timer timer;

    std::unique_ptr<QueryResult> query_result;
    auto status = db_->ScanTable("benchmark", &query_result);

    result.duration_seconds = timer.elapsed_seconds();
    result.success = status.ok();
    result.rows_per_sec = num_rows / result.duration_seconds;
    result.vs_target_percent = (result.rows_per_sec / config_.targets.scan_rows_sec) * 100.0;

    return result;
}

BenchmarkResult MarbleBenchHarness::BenchmarkAggregation(size_t num_rows, const std::string& agg_type) {
    BenchmarkResult result;
    result.name = "Aggregation (" + agg_type + ")";
    result.category = "analytics";

    // Placeholder - would implement aggregation here
    result.success = false;
    result.error_message = "Not implemented";

    return result;
}

BenchmarkResult MarbleBenchHarness::BenchmarkZoneMapFiltering(size_t num_rows) {
    BenchmarkResult result;
    result.name = "Zone Map Filtering";
    result.category = "analytics";

    // Placeholder - would implement zone map filtering here
    result.success = false;
    result.error_message = "Not implemented";

    return result;
}

// ============================================================================
// Data Generation
// ============================================================================

std::shared_ptr<arrow::RecordBatch> MarbleBenchHarness::GenerateRandomBatch(
    size_t num_rows,
    const std::string& schema_type) {

    arrow::Int64Builder id_builder;
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder value_builder;

    int64_t base_ts = std::time(nullptr) * 1000;

    for (size_t i = 0; i < num_rows; ++i) {
        id_builder.Append(id_dist_(rng_)).ok();
        timestamp_builder.Append(base_ts + i).ok();
        value_builder.Append(value_dist_(rng_)).ok();
    }

    std::shared_ptr<arrow::Array> id_array, ts_array, value_array;
    id_builder.Finish(&id_array).ok();
    timestamp_builder.Finish(&ts_array).ok();
    value_builder.Finish(&value_array).ok();

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("value", arrow::float64())
    });

    return arrow::RecordBatch::Make(schema, num_rows,
        {id_array, ts_array, value_array});
}

std::shared_ptr<Record> MarbleBenchHarness::GenerateRandomRecord(int64_t id) {
    // Placeholder - would use BenchRecordImpl from common.hpp
    return nullptr;
}

std::vector<Key> MarbleBenchHarness::GenerateRandomKeys(size_t count) {
    // Placeholder - would use BenchKey from common.hpp
    return {};
}

// ============================================================================
// Database Operations
// ============================================================================

Status MarbleBenchHarness::OpenOrCreateDatabase() {
    return OpenDatabase(config_.db_path, &db_);
}

Status MarbleBenchHarness::CloseDatabase() {
    if (db_) {
        db_.reset();
    }
    return Status::OK();
}

Status MarbleBenchHarness::CreateBenchmarkTable(const std::string& table_name) {
    if (!db_) {
        return Status::InvalidArgument("Database not open");
    }

    TableSchema schema;
    schema.table_name = table_name;
    schema.arrow_schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("value", arrow::float64())
    });

    return db_->CreateTable(schema);
}

// ============================================================================
// Results and Output
// ============================================================================

void MarbleBenchHarness::RecordResult(const BenchmarkResult& result) {
    suite_.results.push_back(result);
}

void MarbleBenchHarness::CalculateSummaryStats() {
    suite_.total_tests = suite_.results.size();
    suite_.passed_tests = 0;
    suite_.failed_tests = 0;

    for (const auto& result : suite_.results) {
        if (result.success) {
            suite_.passed_tests++;
        } else {
            suite_.failed_tests++;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    suite_.total_duration_seconds =
        std::chrono::duration<double>(end - suite_start_time_).count();
}

void MarbleBenchHarness::PrintResults() const {
    if (std::find(config_.output_formats.begin(), config_.output_formats.end(), "console")
        != config_.output_formats.end()) {
        PrintSummary();
    }

    if (std::find(config_.output_formats.begin(), config_.output_formats.end(), "json")
        != config_.output_formats.end()) {
        if (!config_.json_output_file.empty()) {
            ExportJSON(config_.json_output_file);
        } else {
            std::cout << "\n" << SuiteToJSON() << "\n";
        }
    }
}

Status MarbleBenchHarness::ExportJSON(const std::string& filename) const {
    std::ofstream file(filename);
    if (!file.is_open()) {
        return Status::IOError("Cannot open file: " + filename);
    }

    file << SuiteToJSON();
    file.close();

    std::cout << "Results exported to: " << filename << "\n";
    return Status::OK();
}

void MarbleBenchHarness::PrintHeader(const std::string& category) const {
    std::cout << "==========================================\n";
    std::cout << category << "\n";
    std::cout << "==========================================\n";
}

void MarbleBenchHarness::PrintResult(const BenchmarkResult& result) const {
    std::cout << std::left << std::setw(30) << result.name << ": ";

    if (result.success) {
        if (result.operations_per_sec > 0) {
            std::cout << FormatThroughput(result.operations_per_sec) << " ops/sec";
        }
        if (result.rows_per_sec > 0) {
            std::cout << "  (" << FormatThroughput(result.rows_per_sec) << " rows/sec)";
        }
        if (result.vs_target_percent > 0) {
            std::cout << "  [" << std::fixed << std::setprecision(1)
                      << result.vs_target_percent << "% of target]";
        }
    } else {
        std::cout << "❌ FAILED";
        if (!result.error_message.empty()) {
            std::cout << ": " << result.error_message;
        }
    }

    std::cout << "\n";
}

void MarbleBenchHarness::PrintSummary() const {
    std::cout << "\n==========================================\n";
    std::cout << "Summary\n";
    std::cout << "==========================================\n";
    std::cout << "Total tests:   " << suite_.total_tests << "\n";
    std::cout << "Passed:        " << suite_.passed_tests << "\n";
    std::cout << "Failed:        " << suite_.failed_tests << "\n";
    std::cout << "Duration:      " << std::fixed << std::setprecision(2)
              << suite_.total_duration_seconds << " seconds\n";
    std::cout << "==========================================\n";
}

std::string MarbleBenchHarness::FormatThroughput(double ops_per_sec) const {
    std::stringstream ss;
    if (ops_per_sec >= 1000000) {
        ss << std::fixed << std::setprecision(2) << (ops_per_sec / 1000000.0) << "M";
    } else if (ops_per_sec >= 1000) {
        ss << std::fixed << std::setprecision(1) << (ops_per_sec / 1000.0) << "K";
    } else {
        ss << std::fixed << std::setprecision(0) << ops_per_sec;
    }
    return ss.str();
}

std::string MarbleBenchHarness::FormatLatency(double ms) const {
    std::stringstream ss;
    ss << std::fixed << std::setprecision(3) << ms << " ms";
    return ss.str();
}

std::string MarbleBenchHarness::FormatBytes(size_t bytes) const {
    std::stringstream ss;
    if (bytes >= 1024 * 1024 * 1024) {
        ss << std::fixed << std::setprecision(2) << (bytes / (1024.0 * 1024.0 * 1024.0)) << " GB";
    } else if (bytes >= 1024 * 1024) {
        ss << std::fixed << std::setprecision(2) << (bytes / (1024.0 * 1024.0)) << " MB";
    } else if (bytes >= 1024) {
        ss << std::fixed << std::setprecision(2) << (bytes / 1024.0) << " KB";
    } else {
        ss << bytes << " B";
    }
    return ss.str();
}

std::string MarbleBenchHarness::ResultToJSON(const BenchmarkResult& result) const {
    std::stringstream json;
    json << "{\n";
    json << "  \"name\": \"" << result.name << "\",\n";
    json << "  \"category\": \"" << result.category << "\",\n";
    json << "  \"success\": " << (result.success ? "true" : "false") << ",\n";
    json << "  \"operations_per_sec\": " << result.operations_per_sec << ",\n";
    json << "  \"rows_per_sec\": " << result.rows_per_sec << ",\n";
    json << "  \"duration_seconds\": " << result.duration_seconds << ",\n";
    json << "  \"vs_target_percent\": " << result.vs_target_percent << "\n";
    json << "}";
    return json.str();
}

std::string MarbleBenchHarness::SuiteToJSON() const {
    std::stringstream json;
    json << "{\n";
    json << "  \"timestamp\": \"" << suite_.timestamp << "\",\n";
    json << "  \"total_tests\": " << suite_.total_tests << ",\n";
    json << "  \"passed_tests\": " << suite_.passed_tests << ",\n";
    json << "  \"failed_tests\": " << suite_.failed_tests << ",\n";
    json << "  \"total_duration_seconds\": " << suite_.total_duration_seconds << ",\n";
    json << "  \"results\": [\n";

    for (size_t i = 0; i < suite_.results.size(); ++i) {
        json << "    " << ResultToJSON(suite_.results[i]);
        if (i < suite_.results.size() - 1) {
            json << ",";
        }
        json << "\n";
    }

    json << "  ]\n";
    json << "}\n";
    return json.str();
}

} // namespace bench
} // namespace marble

// ============================================================================
// Main Function
// ============================================================================

int main(int argc, char** argv) {
    using namespace marble::bench;

    MarbleBenchHarness harness;

    // Load configuration
    BenchmarkConfig config;
    auto status = ParseCommandLineArgs(argc, argv, &config);
    if (!status.ok()) {
        std::cerr << "Failed to parse arguments: " << status.message() << "\n";
        return 1;
    }

    harness.SetConfig(config);

    // Initialize
    status = harness.Initialize();
    if (!status.ok()) {
        std::cerr << "Initialization failed: " << status.message() << "\n";
        return 1;
    }

    // Run benchmarks
    status = harness.RunBenchmarks();
    if (!status.ok()) {
        std::cerr << "Benchmarks failed: " << status.message() << "\n";
        return 1;
    }

    // Print results
    harness.PrintResults();

    // Cleanup
    harness.Cleanup();

    return 0;
}
