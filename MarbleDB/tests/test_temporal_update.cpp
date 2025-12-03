/**
 * Temporal Update/Delete Test
 *
 * Tests that temporal updates and deletes work correctly:
 * 1. Insert initial data
 * 2. Temporal update closes old versions and inserts new
 * 3. Temporal delete creates tombstone records
 * 4. All versions are preserved for time-travel queries
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

// Helper to create key batch for update/delete operations
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

// Helper to print batch contents
void PrintBatch(const std::shared_ptr<arrow::Table>& table, const std::string& label) {
    std::cout << "\n--- " << label << " ---" << std::endl;
    std::cout << "Rows: " << table->num_rows() << ", Columns: " << table->num_columns() << std::endl;

    // Print column names
    std::cout << "Schema: ";
    for (int i = 0; i < table->num_columns(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << table->field(i)->name();
    }
    std::cout << std::endl;

    // Print first few rows of key columns
    auto emp_id_col = table->GetColumnByName("employee_id");
    auto salary_col = table->GetColumnByName("salary");
    auto sys_end_col = table->GetColumnByName("_system_time_end");
    auto is_deleted_col = table->GetColumnByName("_is_deleted");

    if (emp_id_col && salary_col) {
        for (int64_t row = 0; row < std::min(table->num_rows(), int64_t(10)); ++row) {
            std::cout << "  Row " << row << ": emp_id=";

            // Get chunked array and find the right chunk/index
            int chunk_idx = 0;
            int64_t offset = row;
            while (chunk_idx < emp_id_col->num_chunks() &&
                   offset >= emp_id_col->chunk(chunk_idx)->length()) {
                offset -= emp_id_col->chunk(chunk_idx)->length();
                chunk_idx++;
            }

            if (chunk_idx < emp_id_col->num_chunks()) {
                auto emp_chunk = std::static_pointer_cast<arrow::StringArray>(
                    emp_id_col->chunk(chunk_idx));
                std::cout << emp_chunk->GetString(offset);
            }

            // Find salary in the same manner
            chunk_idx = 0;
            offset = row;
            while (chunk_idx < salary_col->num_chunks() &&
                   offset >= salary_col->chunk(chunk_idx)->length()) {
                offset -= salary_col->chunk(chunk_idx)->length();
                chunk_idx++;
            }

            if (chunk_idx < salary_col->num_chunks()) {
                auto sal_chunk = std::static_pointer_cast<arrow::DoubleArray>(
                    salary_col->chunk(chunk_idx));
                std::cout << ", salary=" << sal_chunk->Value(offset);
            }

            // Print system_time_end (shows if row was "closed")
            if (sys_end_col) {
                chunk_idx = 0;
                offset = row;
                while (chunk_idx < sys_end_col->num_chunks() &&
                       offset >= sys_end_col->chunk(chunk_idx)->length()) {
                    offset -= sys_end_col->chunk(chunk_idx)->length();
                    chunk_idx++;
                }

                if (chunk_idx < sys_end_col->num_chunks()) {
                    auto end_chunk = std::static_pointer_cast<arrow::UInt64Array>(
                        sys_end_col->chunk(chunk_idx));
                    uint64_t end_val = end_chunk->Value(offset);
                    if (end_val == UINT64_MAX) {
                        std::cout << ", sys_end=MAX";
                    } else {
                        std::cout << ", sys_end=" << end_val;
                    }
                }
            }

            // Print is_deleted flag
            if (is_deleted_col) {
                chunk_idx = 0;
                offset = row;
                while (chunk_idx < is_deleted_col->num_chunks() &&
                       offset >= is_deleted_col->chunk(chunk_idx)->length()) {
                    offset -= is_deleted_col->chunk(chunk_idx)->length();
                    chunk_idx++;
                }

                if (chunk_idx < is_deleted_col->num_chunks()) {
                    auto del_chunk = std::static_pointer_cast<arrow::BooleanArray>(
                        is_deleted_col->chunk(chunk_idx));
                    std::cout << ", deleted=" << (del_chunk->Value(offset) ? "true" : "false");
                }
            }

            std::cout << std::endl;
        }
    }
}

int main() {
    std::cout << "=== Temporal Update/Delete Test ===" << std::endl;

    const std::string db_path = "/tmp/marble_temporal_update_test";

    // Clean up any previous test data
    std::system(("rm -rf " + db_path).c_str());

    // Test 1: Create database and bitemporal table, insert initial data
    std::cout << "\nTest 1: Create bitemporal table and insert initial data" << std::endl;
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

        // Insert initial data
        auto batch = CreateEmployeeBatch(
            {"EMP001", "EMP002", "EMP003"},
            {50000.0, 60000.0, 75000.0},
            {"Engineering", "Sales", "Engineering"}
        );

        status = db->InsertBatch("employees", batch);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to insert batch: " << status.ToString() << std::endl;
            return 1;
        }

        status = db->Flush();
        std::cout << "OK: Inserted 3 initial employee records" << std::endl;
    }

    // Small delay to ensure different timestamps
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Test 2: Temporal update - give EMP001 a raise
    std::cout << "\nTest 2: Temporal update - EMP001 gets a raise" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        // Create key batch identifying which employee to update
        auto key_batch = CreateKeyBatch({"EMP001"});

        // Create updated batch with new salary
        auto updated_batch = CreateEmployeeBatch(
            {"EMP001"},
            {55000.0},  // New salary: $55,000
            {"Engineering"}
        );

        status = db->TemporalUpdate("employees", {"employee_id"}, key_batch, updated_batch);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalUpdate failed: " << status.ToString() << std::endl;
            return 1;
        }

        status = db->Flush();
        std::cout << "OK: Temporal update completed for EMP001" << std::endl;
    }

    // Test 3: Verify data - should see both old (closed) and new versions
    std::cout << "\nTest 3: Verify temporal update results" << std::endl;
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
        PrintBatch(table, "After Update");

        // Should have 5 rows in append-only bitemporal storage:
        // - 3 original rows (kept for time-travel, one superseded)
        // - 1 closed marker for EMP001 old version
        // - 1 new version of EMP001
        // The original EMP001 row is superseded but kept for history reconstruction
        if (table->num_rows() != 5) {
            std::cout << "FAIL: Expected 5 rows after update, got " << table->num_rows() << std::endl;
            return 1;
        }
        std::cout << "OK: Correct row count after temporal update (append-only storage)" << std::endl;
    }

    // Small delay
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Test 4: Temporal delete - EMP002 leaves the company
    std::cout << "\nTest 4: Temporal delete - EMP002 leaves" << std::endl;
    {
        std::unique_ptr<MarbleDB> db;
        auto status = OpenDatabase(db_path, &db);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to reopen database: " << status.ToString() << std::endl;
            return 1;
        }

        // Create key batch for delete
        auto key_batch = CreateKeyBatch({"EMP002"});

        status = db->TemporalDelete("employees", {"employee_id"}, key_batch);
        if (!status.ok()) {
            std::cout << "FAIL: TemporalDelete failed: " << status.ToString() << std::endl;
            return 1;
        }

        status = db->Flush();
        std::cout << "OK: Temporal delete completed for EMP002" << std::endl;
    }

    // Test 5: Verify final state
    std::cout << "\nTest 5: Verify final state" << std::endl;
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
        PrintBatch(table, "Final State");

        // In append-only bitemporal storage, we should have 7 rows:
        // From initial insert (batch 0): 3 rows (EMP001, EMP002, EMP003)
        // From update (batch 1): 1 closed marker for EMP001
        // From update (batch 2): 1 new EMP001 version
        // From delete (batch 3): 1 closed marker for EMP002
        // From delete (batch 4): 1 tombstone for EMP002
        if (table->num_rows() != 7) {
            std::cout << "FAIL: Expected 7 rows after delete, got " << table->num_rows() << std::endl;
            return 1;
        }
        std::cout << "OK: Correct row count after temporal delete (append-only storage)" << std::endl;

        // Verify there's exactly one deleted record
        auto is_deleted_col = table->GetColumnByName("_is_deleted");
        if (is_deleted_col) {
            int deleted_count = 0;
            for (int chunk_idx = 0; chunk_idx < is_deleted_col->num_chunks(); ++chunk_idx) {
                auto chunk = std::static_pointer_cast<arrow::BooleanArray>(
                    is_deleted_col->chunk(chunk_idx));
                for (int64_t i = 0; i < chunk->length(); ++i) {
                    if (chunk->Value(i)) {
                        deleted_count++;
                    }
                }
            }
            if (deleted_count != 1) {
                std::cout << "FAIL: Expected 1 deleted record, found " << deleted_count << std::endl;
                return 1;
            }
            std::cout << "OK: Exactly 1 tombstone record" << std::endl;
        }

        // Count closed records (system_time_end != MAX)
        // Note: Code uses std::numeric_limits<int64_t>::max() as "infinity" for temporal columns
        auto sys_end_col = table->GetColumnByName("_system_time_end");
        if (sys_end_col) {
            int closed_count = 0;
            uint64_t max_time = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
            for (int chunk_idx = 0; chunk_idx < sys_end_col->num_chunks(); ++chunk_idx) {
                auto chunk = std::static_pointer_cast<arrow::UInt64Array>(
                    sys_end_col->chunk(chunk_idx));
                for (int64_t i = 0; i < chunk->length(); ++i) {
                    if (chunk->Value(i) != max_time) {
                        closed_count++;
                    }
                }
            }
            // Should have 2 closed markers: EMP001 closed (from update), EMP002 closed (from delete)
            if (closed_count != 2) {
                std::cout << "FAIL: Expected 2 closed records, found " << closed_count << std::endl;
                return 1;
            }
            std::cout << "OK: Exactly 2 closed (historical) records" << std::endl;
        }
    }

    std::cout << "\n=== All Temporal Update/Delete Tests Passed ===" << std::endl;
    return 0;
}
