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

#include "marble/table.h"
#include <arrow/api.h>
#include <arrow/builder.h>
#include <iostream>
#include <chrono>
#include <random>
#include <memory>

// Resolve name conflicts
using marble::Table;
using marble::Database;
using marble::TableSchema;
using marble::ScanSpec;
using marble::QueryResult;
using marble::TimePartition;
using marble::Status;

using namespace marble;
using namespace arrow;

/**
 * @brief Create sample sensor data for demonstration
 */
std::shared_ptr<RecordBatch> CreateSensorData(int num_rows, int64_t base_timestamp) {
    // Create schema: timestamp, sensor_id, temperature, humidity
    auto schema = arrow::schema({
        field("timestamp", timestamp(TimeUnit::MICRO)),
        field("sensor_id", utf8()),
        field("temperature", float64()),
        field("humidity", float64())
    });

    // Create builders
    TimestampBuilder timestamp_builder(timestamp(TimeUnit::MICRO), default_memory_pool());
    StringBuilder sensor_builder(default_memory_pool());
    DoubleBuilder temp_builder(default_memory_pool());
    DoubleBuilder humidity_builder(default_memory_pool());

    // Generate sample data
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> temp_dist(15.0, 35.0);
    std::uniform_real_distribution<> humidity_dist(30.0, 90.0);
    std::uniform_int_distribution<> sensor_dist(1, 10);

    std::vector<std::string> sensor_ids;
    for (int i = 1; i <= 10; ++i) {
        sensor_ids.push_back("sensor_" + std::to_string(i));
    }

    for (int i = 0; i < num_rows; ++i) {
        // Timestamp with some variation
        int64_t timestamp = base_timestamp + (i * 1000000LL);  // 1 second intervals
        auto status = timestamp_builder.Append(timestamp);
        if (!status.ok()) return nullptr;

        // Sensor ID
        int sensor_idx = sensor_dist(gen);
        status = sensor_builder.Append(sensor_ids[sensor_idx - 1]);
        if (!status.ok()) return nullptr;

        // Temperature and humidity
        status = temp_builder.Append(temp_dist(gen));
        if (!status.ok()) return nullptr;

        status = humidity_builder.Append(humidity_dist(gen));
        if (!status.ok()) return nullptr;
    }

    // Build arrays
    std::shared_ptr<Array> timestamp_array;
    std::shared_ptr<Array> sensor_array;
    std::shared_ptr<Array> temp_array;
    std::shared_ptr<Array> humidity_array;

    auto status = timestamp_builder.Finish(&timestamp_array);
    if (!status.ok()) return nullptr;

    status = sensor_builder.Finish(&sensor_array);
    if (!status.ok()) return nullptr;

    status = temp_builder.Finish(&temp_array);
    if (!status.ok()) return nullptr;

    status = humidity_builder.Finish(&humidity_array);
    if (!status.ok()) return nullptr;

    // Create record batch
    return RecordBatch::Make(schema, num_rows,
                            {timestamp_array, sensor_array, temp_array, humidity_array});
}

/**
 * @brief Demonstrate basic MarbleDB table operations
 */
void demonstrate_basic_table() {
    std::cout << "MarbleDB Basic Table Demo" << std::endl;
    std::cout << "========================" << std::endl;

    // Create database
    auto db = CreateDatabase("/tmp/marble_demo");

    // Define table schema
    auto schema = arrow::schema({
        field("timestamp", timestamp(TimeUnit::MICRO)),
        field("sensor_id", utf8()),
        field("temperature", float64()),
        field("humidity", float64())
    });

    TableSchema table_schema("sensor_data", schema);
    table_schema.time_partition = TimePartition::kDaily;
    table_schema.time_column = "timestamp";

    // Create table
    std::cout << "\n1. Creating table..." << std::endl;
    auto status = db->CreateTable(table_schema);
    if (!status.ok()) {
        std::cerr << "Failed to create table: " << status.ToString() << std::endl;
        return;
    }
    std::cout << "✓ Table 'sensor_data' created successfully" << std::endl;

    // Get table reference
    std::shared_ptr<marble::Table> table;
    status = db->GetTable("sensor_data", &table);
    if (!status.ok()) {
        std::cerr << "Failed to get table: " << status.ToString() << std::endl;
        return;
    }

    // Generate and insert sample data
    std::cout << "\n2. Inserting sample sensor data..." << std::endl;

    // Create base timestamp (today at midnight)
    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    auto now_tm = *std::localtime(&now_time_t);
    now_tm.tm_hour = 0;
    now_tm.tm_min = 0;
    now_tm.tm_sec = 0;
    auto today_time_t = std::mktime(&now_tm);
    auto today = std::chrono::system_clock::from_time_t(today_time_t);
    int64_t base_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        today.time_since_epoch()).count();

    int total_rows = 0;
    for (int batch = 0; batch < 5; ++batch) {
        // Create sample data (1000 rows per batch)
        auto batch_data = CreateSensorData(1000, base_timestamp + (batch * 3600000000LL));  // 1 hour apart

        // Insert batch
        status = table->Append(*batch_data);
        if (!status.ok()) {
            std::cerr << "Failed to append batch " << batch << ": " << status.ToString() << std::endl;
            continue;
        }

        total_rows += batch_data->num_rows();
        std::cout << "✓ Inserted batch " << (batch + 1) << " (" << batch_data->num_rows() << " rows)" << std::endl;
    }

    std::cout << "\n✓ Total inserted: " << total_rows << " rows" << std::endl;

    // Query the data
    std::cout << "\n3. Querying data..." << std::endl;

    ScanSpec scan_spec;
    scan_spec.columns = {"timestamp", "sensor_id", "temperature"};  // Project specific columns
    scan_spec.limit = 10;  // Limit to first 10 rows

    QueryResult result;
    status = table->Scan(scan_spec, &result);
    if (!status.ok()) {
        std::cerr << "Failed to scan table: " << status.ToString() << std::endl;
        return;
    }

    std::cout << "✓ Query returned " << result.total_rows << " rows" << std::endl;

    // Display sample results
    if (result.table && result.table->num_rows() > 0) {
        std::cout << "\nSample results (first " << std::min(5, (int)result.table->num_rows()) << " rows):" << std::endl;
        std::cout << "Timestamp\t\t\tSensor ID\tTemperature" << std::endl;
        std::cout << "--------------------------------------------------" << std::endl;

        auto timestamp_col = result.table->GetColumnByName("timestamp");
        auto sensor_col = result.table->GetColumnByName("sensor_id");
        auto temp_col = result.table->GetColumnByName("temperature");

        if (timestamp_col && sensor_col && temp_col) {
            int display_rows = std::min(5, (int)result.table->num_rows());
            for (int i = 0; i < display_rows; ++i) {
                // Extract values (simplified - would need proper type checking in production)
                auto timestamp_scalar = timestamp_col->GetScalar(i).ValueOrDie();
                auto sensor_scalar = sensor_col->GetScalar(i).ValueOrDie();
                auto temp_scalar = temp_col->GetScalar(i).ValueOrDie();

                std::cout << timestamp_scalar->ToString()
                         << "\t" << sensor_scalar->ToString()
                         << "\t\t" << temp_scalar->ToString() << std::endl;
            }
        }
    }

    // Get table statistics
    std::cout << "\n4. Table statistics..." << std::endl;
    std::unordered_map<std::string, std::string> stats;
    status = table->GetStats(&stats);
    if (status.ok()) {
        for (const auto& [key, value] : stats) {
            std::cout << key << ": " << value << std::endl;
        }
    }

    // List partitions
    std::cout << "\n5. Table partitions..." << std::endl;
    std::vector<std::string> partitions;
    status = table->ListPartitions(&partitions);
    if (status.ok()) {
        std::cout << "Found " << partitions.size() << " partition(s):" << std::endl;
        for (const auto& partition : partitions) {
            std::cout << "  - " << partition << std::endl;
        }
    }

    // Get database statistics
    std::cout << "\n6. Database statistics..." << std::endl;
    stats.clear();
    status = db->GetStats(&stats);
    if (status.ok()) {
        for (const auto& [key, value] : stats) {
            std::cout << key << ": " << value << std::endl;
        }
    }

    std::cout << "\n✅ MarbleDB basic table demo completed successfully!" << std::endl;
    std::cout << "\nKey achievements:" << std::endl;
    std::cout << "• Arrow-native data storage and retrieval" << std::endl;
    std::cout << "• Time-based partitioning" << std::endl;
    std::cout << "• Column projection and basic querying" << std::endl;
    std::cout << "• File-based persistence with Feather format" << std::endl;
    std::cout << "• Table and database management APIs" << std::endl;

    std::cout << "\nNext steps for Phase 1 completion:" << std::endl;
    std::cout << "• Add Flight DoPut ingestion endpoint" << std::endl;
    std::cout << "• Implement basic filter pushdown" << std::endl;
    std::cout << "• Add table schema persistence" << std::endl;
    std::cout << "• Performance benchmarking and optimization" << std::endl;
}

int main(int argc, char** argv) {
    try {
        demonstrate_basic_table();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
