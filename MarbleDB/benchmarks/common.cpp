/**
 * MarbleDB Benchmark Common Infrastructure - Implementation
 */

#include "common.h"
#include <ctime>
#include <sys/stat.h>

namespace marble {
namespace bench {

// ArrowBatchBuilder implementation
std::shared_ptr<arrow::RecordBatch> ArrowBatchBuilder::CreateTestBatch(
    size_t num_rows,
    RandomDataGenerator* rng
) {
    // Create builders
    arrow::Int64Builder id_builder;
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder value_builder;
    arrow::StringBuilder category_builder;

    // Use provided RNG or create temporary one
    std::unique_ptr<RandomDataGenerator> temp_rng;
    if (!rng) {
        temp_rng = std::make_unique<RandomDataGenerator>();
        rng = temp_rng.get();
    }

    // Generate random data
    int64_t base_timestamp = std::time(nullptr) * 1000; // milliseconds since epoch

    for (size_t i = 0; i < num_rows; ++i) {
        id_builder.Append(rng->NextId()).ok();
        timestamp_builder.Append(base_timestamp + i).ok();
        value_builder.Append(rng->NextValue()).ok();
        category_builder.Append("cat_" + std::to_string(i % 10)).ok();
    }

    // Finish arrays
    std::shared_ptr<arrow::Array> id_array, ts_array, value_array, category_array;
    id_builder.Finish(&id_array).ok();
    timestamp_builder.Finish(&ts_array).ok();
    value_builder.Finish(&value_array).ok();
    category_builder.Finish(&category_array).ok();

    // Create batch
    return arrow::RecordBatch::Make(
        GetTestSchema(),
        num_rows,
        {id_array, ts_array, value_array, category_array}
    );
}

std::shared_ptr<arrow::RecordBatch> ArrowBatchBuilder::CreateSequentialBatch(
    int64_t start_id,
    size_t num_rows
) {
    // Create builders
    arrow::Int64Builder id_builder;
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder value_builder;
    arrow::StringBuilder category_builder;

    int64_t base_timestamp = std::time(nullptr) * 1000;

    // Generate sequential data
    for (size_t i = 0; i < num_rows; ++i) {
        id_builder.Append(start_id + i).ok();
        timestamp_builder.Append(base_timestamp + i).ok();
        value_builder.Append(static_cast<double>(i) * 1.5).ok();
        category_builder.Append("cat_" + std::to_string(i % 10)).ok();
    }

    // Finish arrays
    std::shared_ptr<arrow::Array> id_array, ts_array, value_array, category_array;
    id_builder.Finish(&id_array).ok();
    timestamp_builder.Finish(&ts_array).ok();
    value_builder.Finish(&value_array).ok();
    category_builder.Finish(&category_array).ok();

    // Create batch
    return arrow::RecordBatch::Make(
        GetTestSchema(),
        num_rows,
        {id_array, ts_array, value_array, category_array}
    );
}

// MarbleTestBase implementation
Status MarbleTestBase::Initialize(bool clean_on_start) {
    if (clean_on_start) {
        // Clean up existing database directory (simple approach - just remove path)
        // In production, would use proper recursive directory removal
        std::system(("rm -rf " + db_path_).c_str());
    }

    // Open/create database
    auto status = OpenDatabase(db_path_, &db_);
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.message() << std::endl;
        return status;
    }

    std::cout << "Initialized database at: " << db_path_ << std::endl;
    return Status::OK();
}

Status MarbleTestBase::Cleanup() {
    if (db_) {
        CloseDatabase(&db_);
    }
    return Status::OK();
}

Status MarbleTestBase::Put(const std::string& table_name, const std::string& key_str) {
    return InsertRecord(db_.get(), table_name, key_str);
}

Status MarbleTestBase::InsertBatch(const std::string& table_name,
                                   std::shared_ptr<arrow::RecordBatch> batch) {
    if (!db_) {
        return Status::InvalidArgument("Database not initialized");
    }

    // Use the InsertBatch API from SimpleMarbleDB
    return db_->InsertBatch(table_name, batch);
}

Status MarbleTestBase::ScanTable(const std::string& table_name,
                                 std::unique_ptr<QueryResult>* result) {
    if (!db_) {
        return Status::InvalidArgument("Database not initialized");
    }

    return db_->ScanTable(table_name, result);
}

Status MarbleTestBase::CreateTestTable(const std::string& table_name) {
    if (!db_) {
        return Status::InvalidArgument("Database not initialized");
    }

    // Create table with test schema
    auto schema = ArrowBatchBuilder::GetTestSchema();
    TableSchema table_schema(table_name, schema);
    return db_->CreateTable(table_schema);
}

// BenchmarkRunner implementation
void BenchmarkRunner::RunAndReport(
    const std::string& name,
    std::function<Status()> benchmark_func,
    size_t operations,
    size_t total_rows
) {
    BenchmarkMetrics metrics;
    metrics.name = name;
    metrics.operations = operations;
    metrics.total_rows = (total_rows > 0) ? total_rows : operations;

    std::cout << "\nRunning: " << name << " ..." << std::endl;

    Timer timer;
    Status status = benchmark_func();
    metrics.duration_seconds = timer.ElapsedSeconds();

    metrics.success = status.ok();

    if (metrics.success) {
        metrics.ops_per_sec = metrics.operations / metrics.duration_seconds;
        metrics.rows_per_sec = metrics.total_rows / metrics.duration_seconds;
        metrics.avg_latency_us = (metrics.duration_seconds * 1000000.0) / metrics.operations;
    }

    metrics.Print();

    if (!status.ok()) {
        std::cerr << "Benchmark failed: " << status.message() << std::endl;
    }
}

} // namespace bench
} // namespace marble
