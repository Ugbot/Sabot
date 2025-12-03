/**
 * Temporal Scan Test
 *
 * Tests time-travel queries using TemporalScan:
 * 1. "AS OF" system time queries
 * 2. Valid time range queries
 * 3. Filtering deleted records
 */

#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <thread>

#include <arrow/api.h>
#include <arrow/builder.h>
#include "marble/api.h"
#include "marble/db.h"
#include "marble/table.h"
#include "marble/table_capabilities.h"
#include "marble/status.h"

using namespace marble;

// Helper to create test data
std::shared_ptr<arrow::RecordBatch> CreateEmployeeBatch(
    const std::vector<std::string>& emp_ids,
    const std::vector<double>& salaries) {

    arrow::StringBuilder emp_id_builder;
    arrow::DoubleBuilder salary_builder;

    for (size_t i = 0; i < emp_ids.size(); ++i) {
        emp_id_builder.Append(emp_ids[i]).ok();
        salary_builder.Append(salaries[i]).ok();
    }

    std::shared_ptr<arrow::Array> emp_id_array, salary_array;
    emp_id_builder.Finish(&emp_id_array).ok();
    salary_builder.Finish(&salary_array).ok();

    auto schema = arrow::schema({
        arrow::field("employee_id", arrow::utf8()),
        arrow::field("salary", arrow::float64())
    });

    return arrow::RecordBatch::Make(schema, emp_ids.size(),
        {emp_id_array, salary_array});
}

std::shared_ptr<arrow::RecordBatch> CreateKeyBatch(
    const std::vector<std::string>& emp_ids) {

    arrow::StringBuilder emp_id_builder;
    for (const auto& id : emp_ids) {
        emp_id_builder.Append(id).ok();
    }

    std::shared_ptr<arrow::Array> emp_id_array;
    emp_id_builder.Finish(&emp_id_array).ok();

    auto schema = arrow::schema({
        arrow::field("employee_id", arrow::utf8())
    });

    return arrow::RecordBatch::Make(schema, emp_ids.size(), {emp_id_array});
}

int main() {
    std::cout << "=== Temporal Scan Test ===" << std::endl;

    const std::string db_path = "/tmp/marble_temporal_scan_test";

    // Clean up any previous test data
    std::system(("rm -rf " + db_path).c_str());

    // Capture timestamps for time-travel queries
    uint64_t time_before_insert = 0;
    uint64_t time_after_insert = 0;
    uint64_t time_after_update = 0;
    uint64_t time_after_delete = 0;

    // Phase 1: Setup - create table and insert initial data
    std::cout << "\nPhase 1: Create table and insert initial data" << std::endl;
    {
        auto now = std::chrono::system_clock::now();
        time_before_insert = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();

        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to open database: " << status.ToString() << std::endl;
            return 1;
        }

        auto schema = arrow::schema({
            arrow::field("employee_id", arrow::utf8()),
            arrow::field("salary", arrow::float64())
        });

        TableSchema table_schema("employees", schema);
        TableCapabilities caps;
        caps.temporal_model = TableCapabilities::TemporalModel::kBitemporal;

        status = db->CreateTable(table_schema, caps);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to create table: " << status.ToString() << std::endl;
            return 1;
        }

        // Insert initial data
        auto batch = CreateEmployeeBatch(
            {"EMP001", "EMP002", "EMP003"},
            {50000.0, 60000.0, 75000.0}
        );

        status = db->InsertBatch("employees", batch);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to insert batch: " << status.ToString() << std::endl;
            return 1;
        }
        status = db->Flush();

        now = std::chrono::system_clock::now();
        time_after_insert = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();

        std::cout << "OK: Inserted 3 employees" << std::endl;
    }

    // Small delay to ensure distinct timestamps
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Phase 2: Update EMP001's salary
    std::cout << "\nPhase 2: Update EMP001's salary" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        auto key_batch = CreateKeyBatch({"EMP001"});
        auto updated_batch = CreateEmployeeBatch({"EMP001"}, {55000.0});

        status = db->TemporalUpdate("employees", {"employee_id"}, key_batch, updated_batch);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalUpdate failed: " << status.ToString() << std::endl;
            return 1;
        }
        status = db->Flush();

        auto now = std::chrono::system_clock::now();
        time_after_update = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();

        std::cout << "OK: Updated EMP001 salary to $55,000" << std::endl;
    }

    // Small delay
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Phase 3: Delete EMP002
    std::cout << "\nPhase 3: Delete EMP002" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        auto key_batch = CreateKeyBatch({"EMP002"});
        status = db->TemporalDelete("employees", {"employee_id"}, key_batch);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalDelete failed: " << status.ToString() << std::endl;
            return 1;
        }
        status = db->Flush();

        auto now = std::chrono::system_clock::now();
        time_after_delete = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();

        std::cout << "OK: Deleted EMP002" << std::endl;
    }

    // Test 1: TemporalScanDedup - deduplicated current state
    // Uses "latest wins" semantics to return only the most recent version per employee
    std::cout << "\nTest 1: TemporalScanDedup current state (deduplicated)" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        std::unique_ptr<QueryResult> result;
        // Use TemporalScanDedup with employee_id as the business key
        status = db->TemporalScanDedup("employees", {"employee_id"}, 0, 0, UINT64_MAX, false, &result);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalScanDedup failed: " << status.ToString() << std::endl;
            return 1;
        }

        auto table_result = result->GetTable();
        if (!table_result.ok()) {
            std::cout << "FAIL: Failed to get table: " << table_result.status().ToString() << std::endl;
            return 1;
        }

        auto table = table_result.ValueUnsafe();
        std::cout << "    Deduplicated current state: " << table->num_rows() << " rows" << std::endl;

        // With deduplication, we should have exactly 2 rows:
        // - EMP001 (latest version with salary $55,000)
        // - EMP003 (unchanged)
        // - EMP002 is filtered (tombstone with is_deleted=true)
        if (table->num_rows() != 2) {
            std::cout << "FAIL: Expected 2 deduplicated rows, got " << table->num_rows() << std::endl;
            return 1;
        }
        std::cout << "OK: Deduplication correctly shows 2 active employees" << std::endl;
    }

    // Test 2: Deleted records inclusion with deduplication
    std::cout << "\nTest 2: Verify deleted records inclusion (deduplicated)" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        std::unique_ptr<QueryResult> result_no_deleted;
        std::unique_ptr<QueryResult> result_with_deleted;

        // Without deleted (deduplicated)
        status = db->TemporalScanDedup("employees", {"employee_id"}, 0, 0, UINT64_MAX, false, &result_no_deleted);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalScanDedup failed: " << status.ToString() << std::endl;
            return 1;
        }

        // With deleted (deduplicated)
        status = db->TemporalScanDedup("employees", {"employee_id"}, 0, 0, UINT64_MAX, true, &result_with_deleted);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalScanDedup (with deleted) failed: " << status.ToString() << std::endl;
            return 1;
        }

        auto table_no_deleted = result_no_deleted->GetTable().ValueOrDie();
        auto table_with_deleted = result_with_deleted->GetTable().ValueOrDie();

        std::cout << "    Without deleted: " << table_no_deleted->num_rows() << " rows" << std::endl;
        std::cout << "    With deleted: " << table_with_deleted->num_rows() << " rows" << std::endl;

        // Without deleted: 2 rows (EMP001, EMP003)
        // With deleted: 3 rows (EMP001, EMP002 tombstone, EMP003)
        if (table_no_deleted->num_rows() != 2) {
            std::cout << "FAIL: Expected 2 rows without deleted, got " << table_no_deleted->num_rows() << std::endl;
            return 1;
        }
        if (table_with_deleted->num_rows() != 3) {
            std::cout << "FAIL: Expected 3 rows with deleted, got " << table_with_deleted->num_rows() << std::endl;
            return 1;
        }
        std::cout << "OK: Deleted filter with deduplication works correctly" << std::endl;
    }

    // Test 3: Time travel - AS OF time_after_insert (before update/delete)
    // With deduplication, this should show exactly 3 employees
    std::cout << "\nTest 3: Time travel with deduplication - AS OF after initial insert" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        std::unique_ptr<QueryResult> result;
        status = db->TemporalScanDedup("employees", {"employee_id"}, time_after_insert, 0, UINT64_MAX, false, &result);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalScanDedup failed: " << status.ToString() << std::endl;
            return 1;
        }

        auto table_result = result->GetTable();
        auto table = table_result.ValueUnsafe();
        std::cout << "    AS OF after insert (deduplicated): " << table->num_rows() << " rows" << std::endl;

        // At time_after_insert, only the original 3 rows existed
        // With deduplication, we should see exactly 3 employees
        if (table->num_rows() != 3) {
            std::cout << "FAIL: Expected 3 rows at historical time, got " << table->num_rows() << std::endl;
            return 1;
        }
        std::cout << "OK: Time travel with deduplication shows exactly 3 original employees" << std::endl;
    }

    std::cout << "\n=== All Temporal Scan Tests Passed ===" << std::endl;
    return 0;
}
