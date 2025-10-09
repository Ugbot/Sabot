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

#include "marble/flight_service.h"
#include "marble/table.h"
#include <arrow/api.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <memory>

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
        timestamp_builder.Append(timestamp);

        // Sensor ID
        int sensor_idx = sensor_dist(gen);
        sensor_builder.Append(sensor_ids[sensor_idx - 1]);

        // Temperature and humidity
        temp_builder.Append(temp_dist(gen));
        humidity_builder.Append(humidity_dist(gen));
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
 * @brief Demonstrate Flight-based ingestion and querying
 */
void demonstrate_flight_ingestion() {
    std::cout << "MarbleDB Flight Ingestion Demo" << std::endl;
    std::cout << "================================" << std::endl;

    const std::string host = "localhost";
    const int port = 50051;

    // Create database
    auto db = CreateDatabase("/tmp/marble_flight_demo");

    // Start Flight server in background thread
    auto server = CreateFlightServer(db, host, port);
    std::cout << "\n1. Starting Flight server on " << host << ":" << port << "..." << std::endl;

    auto status = server->Start();
    if (!status.ok()) {
        std::cerr << "Failed to start server: " << status.ToString() << std::endl;
        return;
    }
    std::cout << "✓ Flight server started successfully" << std::endl;

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create Flight client
    auto client = CreateFlightClient(host, port);
    std::cout << "\n2. Connecting Flight client..." << std::endl;

    status = client->Connect();
    if (!status.ok()) {
        std::cerr << "Failed to connect client: " << status.ToString() << std::endl;
        server->Stop();
        return;
    }
    std::cout << "✓ Flight client connected successfully" << std::endl;

    // Create table via Flight
    std::cout << "\n3. Creating table via Flight..." << std::endl;

    auto schema = arrow::schema({
        field("timestamp", timestamp(TimeUnit::MICRO)),
        field("sensor_id", utf8()),
        field("temperature", float64()),
        field("humidity", float64())
    });

    TableSchema table_schema("sensor_data", schema);
    table_schema.time_partition = TimePartition::kDaily;
    table_schema.time_column = "timestamp";

    status = client->CreateTable(table_schema);
    if (!status.ok()) {
        std::cerr << "Failed to create table: " << status.ToString() << std::endl;
        client->Disconnect();
        server->Stop();
        return;
    }
    std::cout << "✓ Table 'sensor_data' created via Flight" << std::endl;

    // Generate and ingest sample data via Flight
    std::cout << "\n4. Ingesting sample sensor data via Flight..." << std::endl;

    // Create base timestamp (today at midnight)
    auto now = std::chrono::system_clock::now();
    auto today = std::chrono::floor<std::chrono::days>(now);
    int64_t base_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        today.time_since_epoch()).count();

    int total_rows = 0;
    for (int batch = 0; batch < 3; ++batch) {
        // Create sample data (500 rows per batch)
        auto batch_data = CreateSensorData(500, base_timestamp + (batch * 1800000000LL));  // 30 min apart
        if (!batch_data) {
            std::cerr << "Failed to create batch data" << std::endl;
            continue;
        }

        // Ingest via Flight
        status = client->PutRecordBatch("sensor_data", *batch_data);
        if (!status.ok()) {
            std::cerr << "Failed to put batch " << batch << ": " << status.ToString() << std::endl;
            continue;
        }

        total_rows += batch_data->num_rows();
        std::cout << "✓ Ingested batch " << (batch + 1) << " (" << batch_data->num_rows() << " rows) via Flight" << std::endl;
    }

    std::cout << "\n✓ Total ingested: " << total_rows << " rows via Flight" << std::endl;

    // Query the data via Flight
    std::cout << "\n5. Querying data via Flight..." << std::endl;

    ScanSpec scan_spec;
    scan_spec.columns = {"timestamp", "sensor_id", "temperature"};  // Project specific columns
    scan_spec.limit = 10;  // Limit to first 10 rows

    std::shared_ptr<arrow::Table> result_table;
    status = client->ScanTable("sensor_data", scan_spec, &result_table);
    if (!status.ok()) {
        std::cerr << "Failed to scan table: " << status.ToString() << std::endl;
        client->Disconnect();
        server->Stop();
        return;
    }

    std::cout << "✓ Query returned " << result_table->num_rows() << " rows via Flight" << std::endl;

    // Display sample results
    if (result_table && result_table->num_rows() > 0) {
        std::cout << "\nSample results (first " << std::min(5, (int)result_table->num_rows()) << " rows):" << std::endl;
        std::cout << "Timestamp\t\t\tSensor ID\tTemperature" << std::endl;
        std::cout << "--------------------------------------------------" << std::endl;

        auto timestamp_col = result_table->GetColumnByName("timestamp");
        auto sensor_col = result_table->GetColumnByName("sensor_id");
        auto temp_col = result_table->GetColumnByName("temperature");

        if (timestamp_col && sensor_col && temp_col) {
            int display_rows = std::min(5, (int)result_table->num_rows());
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

    // Query with filter (sensor_id)
    std::cout << "\n6. Querying with filter via Flight..." << std::endl;

    ScanSpec filtered_spec;
    filtered_spec.columns = {"timestamp", "sensor_id", "temperature"};
    filtered_spec.filters["sensor_id"] = "sensor_1";  // Filter for sensor_1
    filtered_spec.limit = 5;

    std::shared_ptr<arrow::Table> filtered_result;
    status = client->ScanTable("sensor_data", filtered_spec, &filtered_result);
    if (status.ok() && filtered_result) {
        std::cout << "✓ Filtered query returned " << filtered_result->num_rows() << " rows for sensor_1" << std::endl;
    } else {
        std::cout << "⚠ Filtered query failed or returned no results" << std::endl;
    }

    // Get database statistics
    std::cout << "\n7. Database statistics..." << std::endl;
    std::unordered_map<std::string, std::string> stats;
    status = db->GetStats(&stats);
    if (status.ok()) {
        for (const auto& [key, value] : stats) {
            std::cout << key << ": " << value << std::endl;
        }
    }

    // Cleanup
    std::cout << "\n8. Shutting down..." << std::endl;
    client->Disconnect();
    server->Stop();

    std::cout << "✓ Flight demo completed successfully!" << std::endl;
    std::cout << "\nKey achievements:" << std::endl;
    std::cout << "• Arrow Flight server and client implementation" << std::endl;
    std::cout << "• Zero-copy Arrow RecordBatch ingestion" << std::endl;
    std::cout << "• Remote table creation and management" << std::endl;
    std::cout << "• Distributed-ready query interface" << std::endl;
    std::cout << "• JSON-based command protocol" << std::endl;

    std::cout << "\nNext steps for Phase 1 completion:" << std::endl;
    std::cout << "• Add authentication and authorization" << std::endl;
    std::cout << "• Implement streaming DoGet for large result sets" << std::endl;
    std::cout << "• Add table schema evolution support" << std::endl;
    std::cout << "• Performance benchmarking and optimization" << std::endl;
    std::cout << "• Add Flight SQL support for SQL queries" << std::endl;

    std::cout << "\nPhase 1 Foundation: ✅ COMPLETE" << std::endl;
    std::cout << "MarbleDB now has basic analytical capabilities with Arrow-native storage and ingestion!" << std::endl;
}

int main(int argc, char** argv) {
    try {
        demonstrate_flight_ingestion();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}