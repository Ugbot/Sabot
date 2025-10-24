/**
 * Test 1.1: MarbleDB Database Lifecycle
 *
 * Tests:
 * - Opening a new database
 * - Closing the database
 * - Reopening an existing database
 * - Verifying data persists across restarts
 */

#include <marble/db.h>
#include <iostream>
#include <cstdlib>
#include <filesystem>

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
        // Return empty table with schema
        std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
        return arrow::Table::Make(result->schema(), empty_columns);
    }

    return arrow::Table::FromRecordBatches(batches);
}

int main() {
    std::cout << "========================================\n";
    std::cout << "Test 1.1: Database Lifecycle\n";
    std::cout << "========================================\n\n";

    std::string test_path = "/tmp/test_marbledb_lifecycle";

    // Clean up any existing database
    std::cout << "Step 1: Clean up old test database...\n";
    std::filesystem::remove_all(test_path);
    Pass("Old database removed");

    // Test 1: Open new database
    std::cout << "\nStep 2: Open new database...\n";
    marble::DBOptions options;
    options.db_path = test_path;
    options.enable_wal = true;
    options.enable_sparse_index = true;
    options.enable_bloom_filter = true;

    std::unique_ptr<marble::MarbleDB> db1;
    auto status = marble::MarbleDB::Open(options, nullptr, &db1);
    if (!status.ok()) {
        Fail("Failed to open new database: " + status.ToString());
    }
    Pass("New database opened");

    // Note: Database directory may be created lazily
    std::cout << "  Database directory: " << (std::filesystem::exists(test_path) ? "exists" : "will be created on first write") << "\n";

    // Test 2: Create a column family
    std::cout << "\nStep 3: Create column family...\n";
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::utf8())
    });

    marble::ColumnFamilyOptions cf_opts;
    cf_opts.schema = schema;
    cf_opts.enable_bloom_filter = true;

    marble::ColumnFamilyDescriptor cf_desc("test_cf", cf_opts);
    marble::ColumnFamilyHandle* cf_handle = nullptr;

    status = db1->CreateColumnFamily(cf_desc, &cf_handle);
    if (!status.ok()) {
        Fail("Failed to create column family: " + status.ToString());
    }
    Pass("Column family created");

    // Test 3: Insert some test data
    std::cout << "\nStep 4: Insert test data...\n";
    arrow::Int64Builder id_builder;
    arrow::StringBuilder value_builder;

    if (!id_builder.Append(1).ok() || !value_builder.Append("test_value").ok()) {
        Fail("Failed to append values");
    }

    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> value_array;

    if (!id_builder.Finish(&id_array).ok() || !value_builder.Finish(&value_array).ok()) {
        Fail("Failed to finish arrays");
    }

    auto batch = arrow::RecordBatch::Make(schema, 1, {id_array, value_array});

    status = db1->InsertBatch("test_cf", batch);
    if (!status.ok()) {
        Fail("Failed to insert batch: " + status.ToString());
    }
    Pass("Test data inserted");

    // Test 4: Close database
    std::cout << "\nStep 5: Close database...\n";
    db1.reset();  // Close by destroying the unique_ptr
    Pass("Database closed");

    // Test 5: Reopen database
    std::cout << "\nStep 6: Reopen database...\n";
    std::unique_ptr<marble::MarbleDB> db2;
    status = marble::MarbleDB::Open(options, nullptr, &db2);
    if (!status.ok()) {
        Fail("Failed to reopen database: " + status.ToString());
    }
    Pass("Database reopened");

    // Test 6: Verify column family still exists
    std::cout << "\nStep 7: Verify column family persisted...\n";
    auto cf_list = db2->ListColumnFamilies();
    bool found_cf = false;
    for (const auto& name : cf_list) {
        if (name == "test_cf") {
            found_cf = true;
            break;
        }
    }
    if (!found_cf) {
        Fail("Column family 'test_cf' not found after reopen");
    }
    Pass("Column family persisted");

    // Test 7: Verify data persisted
    std::cout << "\nStep 8: Verify data persisted...\n";
    std::unique_ptr<marble::QueryResult> result;
    status = db2->ScanTable("test_cf", &result);
    if (!status.ok()) {
        Fail("Failed to scan table: " + status.ToString());
    }

    auto table_result = QueryResultToTable(result.get());
    if (!table_result.ok()) {
        Fail("Failed to convert result to table: " + table_result.status().ToString());
    }
    auto result_table = *table_result;

    if (result_table->num_rows() != 1) {
        Fail("Expected 1 row, got " + std::to_string(result_table->num_rows()));
    }
    Pass("Data persisted across restart");

    // Test 8: Clean shutdown
    std::cout << "\nStep 9: Final cleanup...\n";
    db2.reset();
    Pass("Database closed cleanly");

    std::cout << "\n========================================\n";
    std::cout << "✅ ALL TESTS PASSED\n";
    std::cout << "========================================\n";
    std::cout << "\nVerified:\n";
    std::cout << "  - Database can be created\n";
    std::cout << "  - Column families can be created\n";
    std::cout << "  - Data can be inserted\n";
    std::cout << "  - Database can be closed\n";
    std::cout << "  - Database can be reopened\n";
    std::cout << "  - Column families persist\n";
    std::cout << "  - Data persists across restarts\n";

    return 0;
}
