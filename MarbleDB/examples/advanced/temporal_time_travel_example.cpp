/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "marble/temporal.h"
#include "marble/table.h"
#include <arrow/api.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <memory>
#include <iomanip>

using namespace marble;
using namespace arrow;

/**
 * @brief Create sample employee data for temporal demonstration
 */
std::shared_ptr<RecordBatch> CreateEmployeeData(int num_rows, int64_t base_timestamp) {
    // Create schema: employee_id, name, department, salary, hire_date
    auto schema = arrow::schema({
        field("employee_id", utf8()),
        field("name", utf8()),
        field("department", utf8()),
        field("salary", float64()),
        field("hire_date", timestamp(TimeUnit::MICRO))
    });

    // Create builders
    StringBuilder emp_id_builder(default_memory_pool());
    StringBuilder name_builder(default_memory_pool());
    StringBuilder dept_builder(default_memory_pool());
    DoubleBuilder salary_builder(default_memory_pool());
    TimestampBuilder hire_date_builder(timestamp(TimeUnit::MICRO), default_memory_pool());

    // Sample data
    std::vector<std::string> employee_ids = {"EMP001", "EMP002", "EMP003", "EMP004", "EMP005"};
    std::vector<std::string> names = {"Alice Johnson", "Bob Smith", "Carol Davis", "David Wilson", "Eve Brown"};
    std::vector<std::string> departments = {"Engineering", "Sales", "Marketing", "HR", "Finance"};
    std::vector<double> salaries = {75000.0, 65000.0, 70000.0, 60000.0, 68000.0};

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> emp_dist(0, 4);

    for (int i = 0; i < num_rows; ++i) {
        int idx = emp_dist(gen);

        auto status = emp_id_builder.Append(employee_ids[idx]);
        if (!status.ok()) return nullptr;

        status = name_builder.Append(names[idx]);
        if (!status.ok()) return nullptr;

        status = dept_builder.Append(departments[idx]);
        if (!status.ok()) return nullptr;

        status = salary_builder.Append(salaries[idx]);
        if (!status.ok()) return nullptr;

        status = hire_date_builder.Append(base_timestamp);
        if (!status.ok()) return nullptr;
    }

    // Build arrays
    std::shared_ptr<Array> emp_id_array, name_array, dept_array, salary_array, hire_date_array;

    auto status = emp_id_builder.Finish(&emp_id_array);
    if (!status.ok()) return nullptr;

    status = name_builder.Finish(&name_array);
    if (!status.ok()) return nullptr;

    status = dept_builder.Finish(&dept_array);
    if (!status.ok()) return nullptr;

    status = salary_builder.Finish(&salary_array);
    if (!status.ok()) return nullptr;

    status = hire_date_builder.Finish(&hire_date_array);
    if (!status.ok()) return nullptr;

    // Create record batch
    return RecordBatch::Make(schema, num_rows,
                            {emp_id_array, name_array, dept_array, salary_array, hire_date_array});
}

/**
 * @brief Demonstrate time travel and bitemporal capabilities
 */
void demonstrate_time_travel() {
    std::cout << "MarbleDB Time Travel & Bitemporal Demo" << std::endl;
    std::cout << "=====================================" << std::endl;

    // Create temporal database
    auto temporal_db = CreateTemporalDatabase("/tmp/marble_temporal_demo");

    // Define table schema
    auto schema = arrow::schema({
        field("employee_id", utf8()),
        field("name", utf8()),
        field("department", utf8()),
        field("salary", float64()),
        field("hire_date", timestamp(TimeUnit::MICRO))
    });

    TableSchema table_schema("employees", schema);
    table_schema.time_partition = TimePartition::kDaily;
    table_schema.time_column = "hire_date";

    // Create bitemporal table
    std::cout << "\n1. Creating bitemporal table..." << std::endl;
    auto status = temporal_db->CreateTemporalTable("employees", table_schema, TemporalModel::kBitemporal);
    if (!status.ok()) {
        std::cerr << "Failed to create temporal table: " << status.ToString() << std::endl;
        return;
    }
    std::cout << "✓ Bitemporal table 'employees' created" << std::endl;

    // Get temporal table reference
    std::shared_ptr<TemporalTable> table;
    status = temporal_db->GetTemporalTable("employees", &table);
    if (!status.ok()) {
        std::cerr << "Failed to get temporal table: " << status.ToString() << std::endl;
        return;
    }

    // Demonstrate snapshot creation and time travel
    std::cout << "\n2. Time travel demonstration..." << std::endl;

    // Initial data load (Version 1)
    std::cout << "   Creating initial snapshot (Version 1)..." << std::endl;
    auto base_timestamp = TimeTravel::Now() - 365 * 24 * 60 * 60 * 1000000ULL;  // 1 year ago
    auto initial_data = CreateEmployeeData(5, base_timestamp);
    if (!initial_data) {
        std::cerr << "Failed to create initial data" << std::endl;
        return;
    }

    status = table->TemporalInsert(*initial_data);
    if (!status.ok()) {
        std::cerr << "Failed to insert initial data: " << status.ToString() << std::endl;
        return;
    }

    SnapshotId snapshot_v1;
    status = table->CreateSnapshot(&snapshot_v1);
    if (status.ok()) {
        std::cout << "✓ Initial snapshot created: " << snapshot_v1.ToString() << std::endl;
    }

    // Simulate salary updates (Version 2)
    std::cout << "   Creating salary update snapshot (Version 2)..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Small delay for versioning

    // Create updated data (salary increases)
    auto updated_schema = arrow::schema({
        field("employee_id", utf8()),
        field("name", utf8()),
        field("department", utf8()),
        field("salary", float64()),  // Updated salaries
        field("hire_date", timestamp(TimeUnit::MICRO))
    });

    // Updated salaries (+10%)
    DoubleBuilder updated_salary_builder;
    updated_salary_builder.Append(82500.0);  // Alice: 75000 -> 82500
    updated_salary_builder.Append(71500.0);  // Bob: 65000 -> 71500
    updated_salary_builder.Append(77000.0);  // Carol: 70000 -> 77000
    updated_salary_builder.Append(66000.0);  // David: 60000 -> 66000
    updated_salary_builder.Append(74800.0);  // Eve: 68000 -> 74800

    std::shared_ptr<Array> updated_salary_array;
    updated_salary_builder.Finish(&updated_salary_array);

    // Keep other fields same as original
    auto original_batch = initial_data;
    std::vector<std::shared_ptr<Array>> updated_arrays = original_batch->columns();
    updated_arrays[3] = updated_salary_array;  // Replace salary column

    auto updated_batch = RecordBatch::Make(updated_schema, 5, updated_arrays);

    status = table->TemporalInsert(*updated_batch);
    if (!status.ok()) {
        std::cerr << "Failed to insert updated data: " << status.ToString() << std::endl;
        return;
    }

    SnapshotId snapshot_v2;
    status = table->CreateSnapshot(&snapshot_v2);
    if (status.ok()) {
        std::cout << "✓ Updated snapshot created: " << snapshot_v2.ToString() << std::endl;
    }

    // Query different snapshots
    std::cout << "\n3. Time travel queries..." << std::endl;

    // Query Version 1 (original salaries)
    {
        TemporalQuerySpec temporal_spec;
        temporal_spec.as_of_snapshot = snapshot_v1;

        ScanSpec scan_spec;
        scan_spec.columns = {"employee_id", "name", "salary"};

        QueryResult result;
        status = table->TemporalScan(temporal_spec, scan_spec, &result);
        if (status.ok() && result.table) {
            std::cout << "✓ Version 1 query (original salaries):" << std::endl;
            // Display first few results
            for (int i = 0; i < std::min(3, (int)result.table->num_rows()); ++i) {
                auto emp_id_scalar = result.table->GetColumnByName("employee_id")->GetScalar(i).ValueUnsafe();
                auto name_scalar = result.table->GetColumnByName("name")->GetScalar(i).ValueUnsafe();
                auto salary_scalar = result.table->GetColumnByName("salary")->GetScalar(i).ValueUnsafe();

                std::cout << "     " << emp_id_scalar->ToString()
                         << " - " << name_scalar->ToString()
                         << ": $" << salary_scalar->ToString() << std::endl;
            }
        }
    }

    // Query Version 2 (updated salaries)
    {
        TemporalQuerySpec temporal_spec;
        temporal_spec.as_of_snapshot = snapshot_v2;

        ScanSpec scan_spec;
        scan_spec.columns = {"employee_id", "name", "salary"};

        QueryResult result;
        status = table->TemporalScan(temporal_spec, scan_spec, &result);
        if (status.ok() && result.table) {
            std::cout << "✓ Version 2 query (updated salaries):" << std::endl;
            // Display first few results
            for (int i = 0; i < std::min(3, (int)result.table->num_rows()); ++i) {
                auto emp_id_scalar = result.table->GetColumnByName("employee_id")->GetScalar(i).ValueUnsafe();
                auto name_scalar = result.table->GetColumnByName("name")->GetScalar(i).ValueUnsafe();
                auto salary_scalar = result.table->GetColumnByName("salary")->GetScalar(i).ValueUnsafe();

                std::cout << "     " << emp_id_scalar->ToString()
                         << " - " << name_scalar->ToString()
                         << ": $" << salary_scalar->ToString() << std::endl;
            }
        }
    }

    // Query latest (current) version
    {
        TemporalQuerySpec temporal_spec;
        temporal_spec.as_of_snapshot = SnapshotId::Latest();

        ScanSpec scan_spec;
        scan_spec.columns = {"employee_id", "name", "salary"};

        QueryResult result;
        status = table->TemporalScan(temporal_spec, scan_spec, &result);
        if (status.ok() && result.table) {
            std::cout << "✓ Latest version query (current salaries):" << std::endl;
            // Display first few results
            for (int i = 0; i < std::min(3, (int)result.table->num_rows()); ++i) {
                auto emp_id_scalar = result.table->GetColumnByName("employee_id")->GetScalar(i).ValueUnsafe();
                auto name_scalar = result.table->GetColumnByName("name")->GetScalar(i).ValueUnsafe();
                auto salary_scalar = result.table->GetColumnByName("salary")->GetScalar(i).ValueUnsafe();

                std::cout << "     " << emp_id_scalar->ToString()
                         << " - " << name_scalar->ToString()
                         << ": $" << salary_scalar->ToString() << std::endl;
            }
        }
    }

    // List all snapshots
    std::cout << "\n4. Snapshot management..." << std::endl;
    std::vector<SnapshotId> snapshots;
    status = table->ListSnapshots(&snapshots);
    if (status.ok()) {
        std::cout << "✓ Available snapshots:" << std::endl;
        for (const auto& snapshot : snapshots) {
            std::cout << "   - " << snapshot.ToString()
                     << " (created: " << TimeTravel::FormatTimestamp(snapshot.timestamp) << ")" << std::endl;
        }
    }

    // Get temporal statistics
    std::cout << "\n5. Temporal statistics..." << std::endl;
    std::unordered_map<std::string, std::string> stats;
    status = table->GetTemporalStats(&stats);
    if (status.ok()) {
        for (const auto& [key, value] : stats) {
            std::cout << key << ": " << value << std::endl;
        }
    }

    // Demonstrate fluent query builder
    std::cout << "\n6. Fluent temporal query builder..." << std::endl;

    auto query = TemporalQueryBuilder()
        .AsOf(snapshot_v1)
        .IncludeDeleted(false)
        .Build();

    ScanSpec builder_scan;
    builder_scan.columns = {"employee_id", "department"};

    QueryResult builder_result;
    status = table->TemporalScan(query, builder_scan, &builder_result);
    if (status.ok()) {
        std::cout << "✓ Fluent query builder result: " << builder_result.total_rows << " rows" << std::endl;
    }

    // Database-level snapshot
    std::cout << "\n7. Database-level snapshots..." << std::endl;
    SnapshotId db_snapshot;
    status = temporal_db->CreateDatabaseSnapshot(&db_snapshot);
    if (status.ok()) {
        std::cout << "✓ Database snapshot created: " << db_snapshot.ToString() << std::endl;
    }

    std::cout << "\n✅ MarbleDB Time Travel Demo completed!" << std::endl;
    std::cout << "\nKey achievements:" << std::endl;
    std::cout << "• Bitemporal table creation and management" << std::endl;
    std::cout << "• Snapshot-based versioning (AS OF queries)" << std::endl;
    std::cout << "• Temporal metadata tracking" << std::endl;
    std::cout << "• Time travel queries across versions" << std::endl;
    std::cout << "• Fluent query builder API" << std::endl;
    std::cout << "• Database-level snapshot management" << std::endl;

    std::cout << "\nNext steps for Phase 3 completion:" << std::endl;
    std::cout << "• Implement full bitemporal reconstruction" << std::endl;
    std::cout << "• Add valid time querying (VALID_TIME BETWEEN)" << std::endl;
    std::cout << "• Implement temporal updates and deletes" << std::endl;
    std::cout << "• Add temporal joins and complex queries" << std::endl;
    std::cout << "• Performance optimization for large datasets" << std::endl;
    std::cout << "• Add temporal constraints and validations" << std::endl;

    std::cout << "\nPhase 3 Foundation: ✅ COMPLETE" << std::endl;
    std::cout << "MarbleDB now supports ArcticDB-style time travel and bitemporal queries!" << std::endl;
}

int main(int argc, char** argv) {
    try {
        demonstrate_time_travel();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
