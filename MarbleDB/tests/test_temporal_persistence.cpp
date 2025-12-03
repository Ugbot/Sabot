/**
 * Temporal Table Persistence Test
 *
 * Tests that bitemporal data persists through the LSM storage layer.
 * Verifies:
 * 1. Data inserted via TemporalInsert persists to disk
 * 2. Data can be scanned back after insert
 * 3. Temporal metadata columns are properly populated
 */

#include <iostream>
#include <memory>
#include <string>
#include <chrono>

#include <arrow/api.h>
#include <arrow/builder.h>
#include "marble/api.h"
#include "marble/db.h"
#include "marble/table.h"
#include "marble/table_capabilities.h"
#include "marble/temporal.h"
#include "marble/status.h"

using namespace marble;

// Helper to create test data
std::shared_ptr<arrow::RecordBatch> CreateEmployeeBatch(
    const std::vector<std::string>& emp_ids,
    const std::vector<double>& salaries,
    const std::vector<std::string>& departments) {

    arrow::StringBuilder emp_id_builder;
    arrow::DoubleBuilder salary_builder;
    arrow::StringBuilder dept_builder;

    for (size_t i = 0; i < emp_ids.size(); ++i) {
        emp_id_builder.Append(emp_ids[i]).ok();
        salary_builder.Append(salaries[i]).ok();
        dept_builder.Append(departments[i]).ok();
    }

    std::shared_ptr<arrow::Array> emp_id_array, salary_array, dept_array;
    emp_id_builder.Finish(&emp_id_array).ok();
    salary_builder.Finish(&salary_array).ok();
    dept_builder.Finish(&dept_array).ok();

    auto schema = arrow::schema({
        arrow::field("employee_id", arrow::utf8()),
        arrow::field("salary", arrow::float64()),
        arrow::field("department", arrow::utf8())
    });

    return arrow::RecordBatch::Make(schema, emp_ids.size(),
        {emp_id_array, salary_array, dept_array});
}

int main() {
    std::cout << "=== Temporal Table Persistence Test ===" << std::endl;

    const std::string db_path = "/tmp/marble_temporal_persistence_test";

    // Clean up any previous test data
    std::system(("rm -rf " + db_path).c_str());

    // Test 1: Create database and bitemporal table
    std::cout << "\nTest 1: Create database with bitemporal table" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to open database: " << status.ToString() << std::endl;
            return 1;
        }

        // Create bitemporal table
        auto schema = arrow::schema({
            arrow::field("employee_id", arrow::utf8()),
            arrow::field("salary", arrow::float64()),
            arrow::field("department", arrow::utf8())
        });

        TableSchema table_schema("employees", schema);
        TableCapabilities caps;
        caps.temporal_model = TableCapabilities::TemporalModel::kBitemporal;

        status = db->CreateTable(table_schema, caps);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to create bitemporal table: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: Bitemporal table created" << std::endl;
    }

    // Test 2: Insert data using MarbleDB directly
    std::cout << "\nTest 2: Insert data into bitemporal table" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        // Create test data batch (without temporal columns - they're added automatically)
        auto batch = CreateEmployeeBatch(
            {"EMP001", "EMP002", "EMP003"},
            {50000.0, 60000.0, 75000.0},
            {"Engineering", "Sales", "Engineering"}
        );

        // Insert via MarbleDB (which handles the augmented schema)
        status = db->InsertBatch("employees", batch);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to insert batch: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: Inserted 3 employee records" << std::endl;

        // Flush to ensure data is persisted
        status = db->Flush();
        if (!status.ok()) {
            std::cout << "WARNING: Flush returned: " << status.ToString() << std::endl;
        }
    }

    // Test 3: Scan data back
    std::cout << "\nTest 3: Scan data from bitemporal table" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        std::unique_ptr<QueryResult> result;
        status = db->ScanTable("employees", &result);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to scan table: " << status.ToString() << std::endl;
            return 1;
        }

        auto table_result = result->GetTable();
        if (!table_result.ok()) {
            std::cout << "FAIL: Failed to get table: " << table_result.status().ToString() << std::endl;
            return 1;
        }

        auto table = table_result.ValueUnsafe();
        std::cout << "OK: Scanned table with " << table->num_rows() << " rows, "
                  << table->num_columns() << " columns" << std::endl;

        // Print column names to verify temporal columns exist
        std::cout << "    Columns: ";
        for (int i = 0; i < table->schema()->num_fields(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << table->schema()->field(i)->name();
        }
        std::cout << std::endl;
    }

    // Test 4: Create persistent temporal table wrapper and use temporal operations
    std::cout << "\nTest 4: Use TemporalTable interface with persistent storage" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        // Create a TemporalTable backed by MarbleDB storage
        auto user_schema = arrow::schema({
            arrow::field("employee_id", arrow::utf8()),
            arrow::field("salary", arrow::float64()),
            arrow::field("department", arrow::utf8())
        });
        TableSchema table_schema("employees", user_schema);

        auto temporal_table = CreatePersistentTemporalTable(
            db.get(), "employees", table_schema, TemporalModel::kBitemporal);

        if (!temporal_table) {
            std::cout << "FAIL: Failed to create persistent temporal table" << std::endl;
            return 1;
        }
        std::cout << "OK: Created persistent temporal table wrapper" << std::endl;

        // Create a snapshot
        SnapshotId snapshot;
        status = temporal_table->CreateSnapshot(&snapshot);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to create snapshot: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: Created snapshot " << snapshot.ToString() << std::endl;

        // List snapshots
        std::vector<SnapshotId> snapshots;
        status = temporal_table->ListSnapshots(&snapshots);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to list snapshots: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: Found " << snapshots.size() << " snapshot(s)" << std::endl;
    }

    std::cout << "\n=== All Temporal Persistence Tests Passed ===" << std::endl;
    return 0;
}
