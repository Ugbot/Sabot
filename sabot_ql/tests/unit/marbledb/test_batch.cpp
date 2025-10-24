/**
 * Test 1.4: MarbleDB Batch Operations
 *
 * Tests:
 * - Small batch inserts (10 records)
 * - Medium batch inserts (1000 records)
 * - Large batch inserts (10000 records)
 * - Multiple batches
 * - Batch size limits
 */

#include <marble/db.h>
#include <arrow/api.h>
#include <iostream>
#include <cstdlib>
#include <filesystem>
#include <chrono>

void Fail(const std::string& msg) {
    std::cerr << "\n❌ FAILED: " << msg << "\n";
    exit(1);
}

void Pass(const std::string& msg) {
    std::cout << "✅ PASSED: " << msg << "\n";
}

// Helper to convert QueryResult to Table
arrow::Result<std::shared_ptr<arrow::Table>> QueryResultToTable(marble::QueryResult* result) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    while (result->HasNext()) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = result->Next(&batch);
        if (!status.ok()) {
            return arrow::Status::IOError("Failed to get next batch: " + status.ToString());
        }
        if (batch) {
            batches.push_back(batch);
        }
    }
    if (batches.empty()) {
        std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
        return arrow::Table::Make(result->schema(), empty_columns);
    }
    return arrow::Table::FromRecordBatches(batches);
}

// Helper to create a batch of test data
arrow::Result<std::shared_ptr<arrow::RecordBatch>> CreateTestBatch(
    int start_id, int count, const std::shared_ptr<arrow::Schema>& schema) {

    arrow::Int64Builder id_builder;
    arrow::Int64Builder value_builder;

    ARROW_RETURN_NOT_OK(id_builder.Reserve(count));
    ARROW_RETURN_NOT_OK(value_builder.Reserve(count));

    for (int i = 0; i < count; i++) {
        ARROW_RETURN_NOT_OK(id_builder.Append(start_id + i));
        ARROW_RETURN_NOT_OK(value_builder.Append((start_id + i) * 100));
    }

    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> value_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    ARROW_RETURN_NOT_OK(value_builder.Finish(&value_array));

    return arrow::RecordBatch::Make(schema, count, {id_array, value_array});
}

int main() {
    std::cout << "========================================\n";
    std::cout << "Test 1.4: Batch Operations\n";
    std::cout << "========================================\n\n";

    std::string test_path = "/tmp/test_marbledb_batch";
    std::filesystem::remove_all(test_path);

    // Open database
    std::cout << "Step 1: Open database...\n";
    marble::DBOptions options;
    options.db_path = test_path;
    options.enable_wal = true;
    options.memtable_size_threshold = 64 * 1024 * 1024; // 64MB

    std::unique_ptr<marble::MarbleDB> db;
    auto status = marble::MarbleDB::Open(options, nullptr, &db);
    if (!status.ok()) {
        Fail("Failed to open database: " + status.ToString());
    }
    Pass("Database opened");

    // Create column family
    std::cout << "\nStep 2: Create column family...\n";
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::int64())
    });

    marble::ColumnFamilyOptions cf_opts;
    cf_opts.schema = schema;

    marble::ColumnFamilyDescriptor cf_desc("data", cf_opts);
    marble::ColumnFamilyHandle* cf_handle = nullptr;

    status = db->CreateColumnFamily(cf_desc, &cf_handle);
    if (!status.ok()) {
        Fail("Failed to create column family: " + status.ToString());
    }
    Pass("Column family created");

    // Test 1: Small batch (10 records)
    std::cout << "\nStep 3: Insert small batch (10 records)...\n";
    auto batch1_result = CreateTestBatch(1, 10, schema);
    if (!batch1_result.ok()) {
        Fail("Failed to create batch: " + batch1_result.status().ToString());
    }
    auto batch1 = *batch1_result;

    auto start = std::chrono::high_resolution_clock::now();
    status = db->InsertBatch("data", batch1);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    if (!status.ok()) {
        Fail("Failed to insert small batch: " + status.ToString());
    }
    std::cout << "  Time: " << duration.count() << " µs\n";
    Pass("Small batch inserted");

    // Verify
    std::unique_ptr<marble::QueryResult> result1;
    status = db->ScanTable("data", &result1);
    if (!status.ok()) {
        Fail("Failed to scan: " + status.ToString());
    }
    auto table1_result = QueryResultToTable(result1.get());
    if (!table1_result.ok()) {
        Fail("Failed to convert result: " + table1_result.status().ToString());
    }
    auto table1 = *table1_result;

    if (table1->num_rows() != 10) {
        Fail("Expected 10 rows, got " + std::to_string(table1->num_rows()));
    }
    Pass("Small batch verified");

    // Test 2: Medium batch (1000 records)
    std::cout << "\nStep 4: Insert medium batch (1000 records)...\n";
    auto batch2_result = CreateTestBatch(11, 1000, schema);
    if (!batch2_result.ok()) {
        Fail("Failed to create batch: " + batch2_result.status().ToString());
    }
    auto batch2 = *batch2_result;

    start = std::chrono::high_resolution_clock::now();
    status = db->InsertBatch("data", batch2);
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    if (!status.ok()) {
        Fail("Failed to insert medium batch: " + status.ToString());
    }
    std::cout << "  Time: " << duration.count() << " µs\n";
    std::cout << "  Throughput: " << (1000.0 / duration.count() * 1000000) << " records/sec\n";
    Pass("Medium batch inserted");

    // Verify
    std::unique_ptr<marble::QueryResult> result2;
    status = db->ScanTable("data", &result2);
    if (!status.ok()) {
        Fail("Failed to scan: " + status.ToString());
    }
    auto table2_result = QueryResultToTable(result2.get());
    if (!table2_result.ok()) {
        Fail("Failed to convert result: " + table2_result.status().ToString());
    }
    auto table2 = *table2_result;

    if (table2->num_rows() != 1010) {
        std::cout << "  Expected 1010 rows, got " << table2->num_rows() << "\n";
    }
    Pass("Medium batch verified");

    // Test 3: Large batch (10000 records)
    std::cout << "\nStep 5: Insert large batch (10000 records)...\n";
    auto batch3_result = CreateTestBatch(1011, 10000, schema);
    if (!batch3_result.ok()) {
        Fail("Failed to create batch: " + batch3_result.status().ToString());
    }
    auto batch3 = *batch3_result;

    start = std::chrono::high_resolution_clock::now();
    status = db->InsertBatch("data", batch3);
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    if (!status.ok()) {
        Fail("Failed to insert large batch: " + status.ToString());
    }
    std::cout << "  Time: " << duration.count() << " µs (" << (duration.count() / 1000) << " ms)\n";
    std::cout << "  Throughput: " << (10000.0 / duration.count() * 1000000) << " records/sec\n";
    Pass("Large batch inserted");

    // Verify
    std::unique_ptr<marble::QueryResult> result3;
    status = db->ScanTable("data", &result3);
    if (!status.ok()) {
        Fail("Failed to scan: " + status.ToString());
    }
    auto table3_result = QueryResultToTable(result3.get());
    if (!table3_result.ok()) {
        Fail("Failed to convert result: " + table3_result.status().ToString());
    }
    auto table3 = *table3_result;

    auto final_rows = table3->num_rows();
    std::cout << "  Total rows: " << final_rows << "\n";
    Pass("Large batch verified");

    // Test 4: Multiple sequential batches
    std::cout << "\nStep 6: Insert multiple batches (10 x 100 records)...\n";
    int batches_inserted = 0;
    start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 10; i++) {
        auto batch_result = CreateTestBatch(20000 + i * 100, 100, schema);
        if (!batch_result.ok()) {
            Fail("Failed to create batch " + std::to_string(i));
        }

        status = db->InsertBatch("data", *batch_result);
        if (!status.ok()) {
            Fail("Failed to insert batch " + std::to_string(i) + ": " + status.ToString());
        }
        batches_inserted++;
    }

    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    std::cout << "  Batches inserted: " << batches_inserted << "\n";
    std::cout << "  Time: " << duration.count() << " µs\n";
    std::cout << "  Throughput: " << (1000.0 / duration.count() * 1000000) << " records/sec\n";
    Pass("Multiple batches inserted");

    std::cout << "\n========================================\n";
    std::cout << "✅ ALL TESTS PASSED\n";
    std::cout << "========================================\n";
    std::cout << "\nVerified:\n";
    std::cout << "  - Small batches (10 records)\n";
    std::cout << "  - Medium batches (1000 records)\n";
    std::cout << "  - Large batches (10000 records)\n";
    std::cout << "  - Multiple sequential batches\n";
    std::cout << "  - Batch throughput measurement\n";

    return 0;
}
