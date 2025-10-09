/**
 * MarbleDB Core Functionality Tests
 *
 * Comprehensive test suite covering:
 * - Basic table operations
 * - Analytical capabilities
 * - Temporal features
 * - Performance characteristics
 */

#include "marble/table.h"
#include "marble/analytics.h"
#include "marble/temporal.h"
#include <arrow/api.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

class MarbleTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_path_ = "/tmp/marble_test_" + std::to_string(std::time(nullptr));
        fs::create_directories(test_path_);
    }

    void TearDown() override {
        fs::remove_all(test_path_);
    }

    std::string test_path_;
};

// Test basic table operations
TEST_F(MarbleTest, BasicTableOperations) {
    // Create database
    auto db = CreateDatabase(test_path_);

    // Define schema
    auto schema = arrow::schema({
        field("id", arrow::int64()),
        field("name", arrow::utf8()),
        field("value", arrow::float64())
    });

    TableSchema table_schema("test_table", schema);

    // Create table
    auto status = db->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    // Get table
    std::shared_ptr<Table> table;
    status = db->GetTable("test_table", &table);
    ASSERT_TRUE(status.ok());

    // Create test data
    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::DoubleBuilder value_builder;

    id_builder.Append(1);
    id_builder.Append(2);
    name_builder.Append("Alice");
    name_builder.Append("Bob");
    value_builder.Append(10.5);
    value_builder.Append(20.3);

    std::shared_ptr<arrow::Array> id_array, name_array, value_array;
    id_builder.Finish(&id_array);
    name_builder.Finish(&name_array);
    value_builder.Finish(&value_array);

    auto batch = arrow::RecordBatch::Make(schema, 2, {id_array, name_array, value_array});

    // Insert data
    status = table->Append(*batch);
    ASSERT_TRUE(status.ok());

    // Query data
    ScanSpec scan_spec;
    scan_spec.columns = {"id", "name", "value"};

    QueryResult result;
    status = table->Scan(scan_spec, &result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result.total_rows, 2);

    // Verify data
    auto id_col = result.table->GetColumnByName("id");
    auto name_col = result.table->GetColumnByName("name");

    ASSERT_TRUE(id_col && name_col);
    ASSERT_EQ(result.table->num_rows(), 2);
}

// Test analytical capabilities
TEST_F(MarbleTest, AnalyticalCapabilities) {
    auto db = CreateDatabase(test_path_);

    auto schema = arrow::schema({
        field("sensor_id", arrow::utf8()),
        field("temperature", arrow::float64()),
        field("humidity", arrow::float64())
    });

    TableSchema table_schema("sensor_data", schema);

    auto status = db->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    std::shared_ptr<Table> table;
    status = db->GetTable("sensor_data", &table);
    ASSERT_TRUE(status.ok());

    // Insert test data
    arrow::StringBuilder sensor_builder;
    arrow::DoubleBuilder temp_builder;
    arrow::DoubleBuilder humidity_builder;

    sensor_builder.Append("sensor_1");
    sensor_builder.Append("sensor_1");
    sensor_builder.Append("sensor_2");
    temp_builder.Append(20.0);
    temp_builder.Append(25.0);
    temp_builder.Append(30.0);
    humidity_builder.Append(60.0);
    humidity_builder.Append(65.0);
    humidity_builder.Append(70.0);

    std::shared_ptr<arrow::Array> sensor_array, temp_array, humidity_array;
    sensor_builder.Finish(&sensor_array);
    temp_builder.Finish(&temp_array);
    humidity_builder.Finish(&humidity_array);

    auto batch = arrow::RecordBatch::Make(schema, 3, {sensor_array, temp_array, humidity_array});
    status = table->Append(*batch);
    ASSERT_TRUE(status.ok());

    // Test aggregations
    AggregationEngine agg_engine;

    std::vector<AggregationEngine::AggSpec> specs = {
        {AggregationEngine::AggFunction::kCount, "sensor_id", "count"},
        {AggregationEngine::AggFunction::kSum, "temperature", "sum_temp"},
        {AggregationEngine::AggFunction::kAvg, "humidity", "avg_humidity"}
    };

    std::shared_ptr<arrow::Table> input_table;
    ScanSpec scan_spec;
    scan_spec.columns = {"sensor_id", "temperature", "humidity"};

    QueryResult input_result;
    status = table->Scan(scan_spec, &input_result);
    ASSERT_TRUE(status.ok());

    std::shared_ptr<arrow::Table> result_table;
    status = agg_engine.Execute(specs, input_result.table, &result_table);
    ASSERT_TRUE(status.ok());

    // Verify aggregation results
    auto count_col = result_table->GetColumnByName("count");
    auto sum_col = result_table->GetColumnByName("sum_temp");
    auto avg_col = result_table->GetColumnByName("avg_humidity");

    ASSERT_TRUE(count_col && sum_col && avg_col);

    auto count_scalar = count_col->GetScalar(0).ValueUnsafe();
    auto sum_scalar = sum_col->GetScalar(0).ValueUnsafe();
    auto avg_scalar = avg_col->GetScalar(0).ValueUnsafe();

    ASSERT_EQ(std::stoi(count_scalar->ToString()), 3);  // Count should be 3
}

// Test temporal features
TEST_F(MarbleTest, TemporalFeatures) {
    auto temporal_db = CreateTemporalDatabase(test_path_);

    auto schema = arrow::schema({
        field("employee_id", arrow::utf8()),
        field("salary", arrow::float64())
    });

    TableSchema table_schema("employees", schema);

    // Create bitemporal table
    auto status = temporal_db->CreateTemporalTable("employees", table_schema, TemporalModel::kBitemporal);
    ASSERT_TRUE(status.ok());

    // Get temporal table
    std::shared_ptr<TemporalTable> table;
    status = temporal_db->GetTemporalTable("employees", &table);
    ASSERT_TRUE(status.ok());

    // Verify temporal model
    ASSERT_EQ(table->GetTemporalModel(), TemporalModel::kBitemporal);

    // Create initial snapshot
    SnapshotId snapshot1;
    status = table->CreateSnapshot(&snapshot1);
    ASSERT_TRUE(status.ok());

    // Insert data
    arrow::StringBuilder emp_builder;
    arrow::DoubleBuilder salary_builder;

    emp_builder.Append("EMP001");
    salary_builder.Append(50000.0);

    std::shared_ptr<arrow::Array> emp_array, salary_array;
    emp_builder.Finish(&emp_array);
    salary_builder.Finish(&salary_array);

    auto batch = arrow::RecordBatch::Make(schema, 1, {emp_array, salary_array});

    TemporalMetadata metadata;
    status = table->TemporalInsert(*batch, metadata);
    ASSERT_TRUE(status.ok());

    // Create second snapshot
    SnapshotId snapshot2;
    status = table->CreateSnapshot(&snapshot2);
    ASSERT_TRUE(status.ok());

    // Query snapshots
    TemporalQuerySpec temporal_spec;
    temporal_spec.as_of_snapshot = snapshot1;

    ScanSpec scan_spec;
    scan_spec.columns = {"employee_id", "salary"};

    QueryResult result;
    status = table->TemporalScan(temporal_spec, scan_spec, &result);
    ASSERT_TRUE(status.ok());

    // List snapshots
    std::vector<SnapshotId> snapshots;
    status = table->ListSnapshots(&snapshots);
    ASSERT_TRUE(status.ok());
    ASSERT_GE(snapshots.size(), 2);
}

// Test query optimization
TEST_F(MarbleTest, QueryOptimization) {
    auto db = CreateDatabase(test_path_);

    auto schema = arrow::schema({
        field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
        field("sensor_id", arrow::utf8()),
        field("value", arrow::float64())
    });

    TableSchema table_schema("sensor_readings", schema);
    table_schema.time_partition = TimePartition::kDaily;

    auto status = db->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    std::shared_ptr<Table> table;
    status = db->GetTable("sensor_readings", &table);
    ASSERT_TRUE(status.ok());

    // Insert data (this will build indexes)
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();

    arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::StringBuilder sensor_builder;
    arrow::DoubleBuilder value_builder;

    ts_builder.Append(timestamp);
    ts_builder.Append(timestamp + 1000000);  // 1 second later
    sensor_builder.Append("sensor_1");
    sensor_builder.Append("sensor_2");
    value_builder.Append(10.5);
    value_builder.Append(20.3);

    std::shared_ptr<arrow::Array> ts_array, sensor_array, value_array;
    ts_builder.Finish(&ts_array);
    sensor_builder.Finish(&sensor_array);
    value_builder.Finish(&value_array);

    auto batch = arrow::RecordBatch::Make(schema, 2, {ts_array, sensor_array, value_array});
    status = table->Append(*batch);
    ASSERT_TRUE(status.ok());

    // Test optimized query
    ScanSpec scan_spec;
    scan_spec.columns = {"sensor_id", "value"};
    scan_spec.filters["sensor_id"] = "sensor_1";

    QueryResult result;
    status = table->Scan(scan_spec, &result);
    ASSERT_TRUE(status.ok());
    // Should return only sensor_1 data
    ASSERT_EQ(result.total_rows, 1);
}

// Test time travel utilities
TEST_F(MarbleTest, TimeTravelUtilities) {
    // Test timestamp functions
    uint64_t now = TimeTravel::Now();
    ASSERT_GT(now, 0ULL);

    std::string formatted = TimeTravel::FormatTimestamp(now);
    ASSERT_FALSE(formatted.empty());

    // Test snapshot ID generation
    SnapshotId id1 = TimeTravel::GenerateSnapshotId();
    SnapshotId id2 = TimeTravel::GenerateSnapshotId();

    ASSERT_NE(id1.timestamp, 0ULL);
    ASSERT_NE(id2.timestamp, 0ULL);
    // IDs should be different (or at least have different timestamps)
    ASSERT_TRUE(id1.timestamp != id2.timestamp || id1.version != id2.version);

    // Test snapshot ID string conversion
    std::string id_str = id1.ToString();
    SnapshotId parsed = SnapshotId::FromString(id_str);

    ASSERT_EQ(parsed.timestamp, id1.timestamp);
    ASSERT_EQ(parsed.version, id1.version);
}

// Performance test
TEST_F(MarbleTest, PerformanceTest) {
    auto db = CreateDatabase(test_path_);

    auto schema = arrow::schema({
        field("id", arrow::int64()),
        field("data", arrow::utf8())
    });

    TableSchema table_schema("perf_test", schema);

    auto status = db->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    std::shared_ptr<Table> table;
    status = db->GetTable("perf_test", &table);
    ASSERT_TRUE(status.ok());

    // Generate larger dataset for performance testing
    const int num_batches = 10;
    const int batch_size = 1000;

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int batch = 0; batch < num_batches; ++batch) {
        arrow::Int64Builder id_builder;
        arrow::StringBuilder data_builder;

        for (int i = 0; i < batch_size; ++i) {
            id_builder.Append(batch * batch_size + i);
            data_builder.Append("data_" + std::to_string(i));
        }

        std::shared_ptr<arrow::Array> id_array, data_array;
        id_builder.Finish(&id_array);
        data_builder.Finish(&data_array);

        auto test_batch = arrow::RecordBatch::Make(schema, batch_size, {id_array, data_array});
        status = table->Append(*test_batch);
        ASSERT_TRUE(status.ok());
    }

    auto insert_end_time = std::chrono::high_resolution_clock::now();

    // Test query performance
    ScanSpec scan_spec;
    scan_spec.columns = {"id"};

    QueryResult result;
    auto query_start_time = std::chrono::high_resolution_clock::now();
    status = table->Scan(scan_spec, &result);
    auto query_end_time = std::chrono::high_resolution_clock::now();

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result.total_rows, num_batches * batch_size);

    auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        insert_end_time - start_time);
    auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        query_end_time - query_start_time);

    std::cout << "Performance Test Results:" << std::endl;
    std::cout << "  Inserted " << (num_batches * batch_size) << " rows in "
              << insert_duration.count() << "ms" << std::endl;
    std::cout << "  Queried " << result.total_rows << " rows in "
              << query_duration.count() << "ms" << std::endl;

    // Basic performance assertions
    ASSERT_LT(insert_duration.count(), 5000);  // Should complete within 5 seconds
    ASSERT_LT(query_duration.count(), 1000);  // Should complete within 1 second
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
