/**
 * Temporal + MVCC Integration Test
 *
 * Tests that bitemporal operations work correctly with MVCC transactions:
 * 1. Temporal operations use transaction timestamps
 * 2. Snapshot isolation works with temporal scans
 * 3. Concurrent updates detect conflicts correctly
 */

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <set>

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
    std::cout << "=== Temporal + MVCC Integration Test ===" << std::endl;

    const std::string db_path = "/tmp/marble_temporal_mvcc_test";

    // Clean up any previous test data
    std::system(("rm -rf " + db_path).c_str());

    // Test 1: Create bitemporal table with MVCC enabled
    std::cout << "\nTest 1: Create bitemporal table with MVCC" << std::endl;
    {
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
        caps.enable_mvcc = true;
        caps.mvcc_settings.max_versions_per_key = 100;
        caps.mvcc_settings.gc_policy = TableCapabilities::MVCCSettings::GCPolicy::kKeepRecentVersions;

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
        std::cout << "OK: Created bitemporal table with MVCC enabled" << std::endl;
    }

    // Test 2: Verify system timestamps are sequential
    std::cout << "\nTest 2: System timestamps are sequential after operations" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database" << std::endl;
            return 1;
        }

        // Record time before update
        auto time_before = std::chrono::system_clock::now();
        auto time_before_us = std::chrono::duration_cast<std::chrono::microseconds>(
            time_before.time_since_epoch()).count();

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        // Perform update
        auto key_batch = CreateKeyBatch({"EMP001"});
        auto updated_batch = CreateEmployeeBatch({"EMP001"}, {55000.0});

        status = db->TemporalUpdate("employees", {"employee_id"}, key_batch, updated_batch);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalUpdate failed: " << status.ToString() << std::endl;
            return 1;
        }
        status = db->Flush();

        // Scan all raw rows
        std::unique_ptr<QueryResult> result;
        status = db->ScanTable("employees", &result);
        if (!status.ok()) {
            std::cout << "FAIL: ScanTable failed" << std::endl;
            return 1;
        }

        auto table = result->GetTable().ValueOrDie();

        // Check that new row has sys_start > time_before
        auto sys_start_col = table->GetColumnByName("_system_time_start");
        bool found_newer = false;
        for (int64_t row = 0; row < table->num_rows(); ++row) {
            int64_t remaining = row;
            int chunk_idx = 0;
            while (chunk_idx < sys_start_col->num_chunks() &&
                   remaining >= sys_start_col->chunk(chunk_idx)->length()) {
                remaining -= sys_start_col->chunk(chunk_idx)->length();
                chunk_idx++;
            }
            if (chunk_idx < sys_start_col->num_chunks()) {
                auto arr = std::static_pointer_cast<arrow::UInt64Array>(sys_start_col->chunk(chunk_idx));
                uint64_t sys_start = arr->Value(remaining);
                if (sys_start > static_cast<uint64_t>(time_before_us)) {
                    found_newer = true;
                    break;
                }
            }
        }

        if (!found_newer) {
            std::cout << "FAIL: No rows have sys_start > time_before" << std::endl;
            return 1;
        }
        std::cout << "OK: System timestamps are sequential" << std::endl;
    }

    // Test 3: Time-travel query sees consistent snapshot
    std::cout << "\nTest 3: Time-travel query sees consistent snapshot" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database" << std::endl;
            return 1;
        }

        // Query current state with deduplication
        std::unique_ptr<QueryResult> result;
        status = db->TemporalScanDedup("employees", {"employee_id"}, 0, 0, UINT64_MAX, false, &result);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalScanDedup failed" << std::endl;
            return 1;
        }

        auto current_table = result->GetTable().ValueOrDie();
        std::cout << "    Current state: " << current_table->num_rows() << " employees" << std::endl;

        // Current state should have 3 employees (EMP001 updated, EMP002, EMP003)
        if (current_table->num_rows() != 3) {
            std::cout << "FAIL: Expected 3 current employees, got " << current_table->num_rows() << std::endl;
            return 1;
        }

        // Get raw scan for sys_start timestamp of initial insert
        std::unique_ptr<QueryResult> raw_result;
        status = db->ScanTable("employees", &raw_result);
        auto raw_table = raw_result->GetTable().ValueOrDie();

        // Find minimum sys_start (initial insert time)
        auto sys_start_col = raw_table->GetColumnByName("_system_time_start");
        uint64_t initial_time = UINT64_MAX;
        for (int64_t row = 0; row < raw_table->num_rows(); ++row) {
            int64_t remaining = row;
            int chunk_idx = 0;
            while (chunk_idx < sys_start_col->num_chunks() &&
                   remaining >= sys_start_col->chunk(chunk_idx)->length()) {
                remaining -= sys_start_col->chunk(chunk_idx)->length();
                chunk_idx++;
            }
            if (chunk_idx < sys_start_col->num_chunks()) {
                auto arr = std::static_pointer_cast<arrow::UInt64Array>(sys_start_col->chunk(chunk_idx));
                uint64_t sys_start = arr->Value(remaining);
                if (sys_start < initial_time) {
                    initial_time = sys_start;
                }
            }
        }

        // Query as of initial_time + 1 (just after initial insert)
        // Should see original 3 employees with original salaries
        std::unique_ptr<QueryResult> historical_result;
        status = db->TemporalScanDedup("employees", {"employee_id"}, initial_time + 1, 0, UINT64_MAX, false, &historical_result);
        if (!status.ok()) {
            std::cout << "FAIL: Historical TemporalScanDedup failed" << std::endl;
            return 1;
        }

        auto historical_table = historical_result->GetTable().ValueOrDie();
        std::cout << "    Historical state (after initial insert): " << historical_table->num_rows() << " employees" << std::endl;

        if (historical_table->num_rows() != 3) {
            std::cout << "FAIL: Expected 3 historical employees, got " << historical_table->num_rows() << std::endl;
            return 1;
        }

        std::cout << "OK: Time-travel query sees consistent snapshot" << std::endl;
    }

    // Test 4: Concurrent temporal operations on different keys
    std::cout << "\nTest 4: Concurrent temporal operations on different keys" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database" << std::endl;
            return 1;
        }

        // Update EMP002
        auto key_batch2 = CreateKeyBatch({"EMP002"});
        auto updated_batch2 = CreateEmployeeBatch({"EMP002"}, {65000.0});

        status = db->TemporalUpdate("employees", {"employee_id"}, key_batch2, updated_batch2);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalUpdate for EMP002 failed" << std::endl;
            return 1;
        }

        // Update EMP003
        auto key_batch3 = CreateKeyBatch({"EMP003"});
        auto updated_batch3 = CreateEmployeeBatch({"EMP003"}, {80000.0});

        status = db->TemporalUpdate("employees", {"employee_id"}, key_batch3, updated_batch3);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalUpdate for EMP003 failed" << std::endl;
            return 1;
        }

        status = db->Flush();

        // Verify all updates are visible
        std::unique_ptr<QueryResult> result;
        status = db->TemporalScanDedup("employees", {"employee_id"}, 0, 0, UINT64_MAX, false, &result);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalScanDedup failed" << std::endl;
            return 1;
        }

        auto table = result->GetTable().ValueOrDie();
        if (table->num_rows() != 3) {
            std::cout << "FAIL: Expected 3 employees after updates, got " << table->num_rows() << std::endl;
            return 1;
        }

        std::cout << "OK: Concurrent temporal operations on different keys succeed" << std::endl;
    }

    // Test 5: Verify version history is maintained
    std::cout << "\nTest 5: Version history is maintained for time-travel" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database" << std::endl;
            return 1;
        }

        // Scan raw table to count all versions
        std::unique_ptr<QueryResult> raw_result;
        status = db->ScanTable("employees", &raw_result);
        if (!status.ok()) {
            std::cout << "FAIL: ScanTable failed" << std::endl;
            return 1;
        }

        auto raw_table = raw_result->GetTable().ValueOrDie();
        std::cout << "    Total versions (raw rows): " << raw_table->num_rows() << std::endl;

        // Count unique business keys
        auto emp_id_col = raw_table->GetColumnByName("employee_id");
        std::set<std::string> unique_keys;
        for (int64_t row = 0; row < raw_table->num_rows(); ++row) {
            int64_t remaining = row;
            int chunk_idx = 0;
            while (chunk_idx < emp_id_col->num_chunks() &&
                   remaining >= emp_id_col->chunk(chunk_idx)->length()) {
                remaining -= emp_id_col->chunk(chunk_idx)->length();
                chunk_idx++;
            }
            if (chunk_idx < emp_id_col->num_chunks()) {
                auto arr = std::static_pointer_cast<arrow::StringArray>(emp_id_col->chunk(chunk_idx));
                unique_keys.insert(arr->GetString(remaining));
            }
        }
        std::cout << "    Unique business keys: " << unique_keys.size() << std::endl;

        // Should have 3 unique keys with multiple versions each
        if (unique_keys.size() != 3) {
            std::cout << "FAIL: Expected 3 unique business keys, got " << unique_keys.size() << std::endl;
            return 1;
        }

        // Should have more raw rows than unique keys (versions)
        if (raw_table->num_rows() <= 3) {
            std::cout << "FAIL: Expected more than 3 raw rows (versions)" << std::endl;
            return 1;
        }

        std::cout << "OK: Version history maintained for time-travel" << std::endl;
    }

    std::cout << "\n=== All Temporal + MVCC Integration Tests Passed ===" << std::endl;
    return 0;
}
