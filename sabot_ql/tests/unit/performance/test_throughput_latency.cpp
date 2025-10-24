/**
 * Test 7.1: Throughput and Latency Benchmarks
 *
 * Tests:
 * - Batch insert throughput (records/sec)
 * - Single operation latency (µs)
 * - Scan throughput (rows/sec)
 * - Range query latency
 * - Mixed workload performance
 */

#include <marble/db.h>
#include <marble/record.h>
#include <arrow/api.h>
#include <iostream>
#include <cstdlib>
#include <filesystem>
#include <chrono>
#include <vector>
#include <random>

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

// Helper to create test batch
arrow::Result<std::shared_ptr<arrow::RecordBatch>> CreateBatch(
    int start, int count, const std::shared_ptr<arrow::Schema>& schema) {

    arrow::Int64Builder id_builder;
    arrow::StringBuilder value_builder;

    for (int i = 0; i < count; i++) {
        ARROW_RETURN_NOT_OK(id_builder.Append(start + i));
        ARROW_RETURN_NOT_OK(value_builder.Append("value_" + std::to_string(start + i)));
    }

    std::shared_ptr<arrow::Array> id_array;
    std::shared_ptr<arrow::Array> value_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    ARROW_RETURN_NOT_OK(value_builder.Finish(&value_array));

    return arrow::RecordBatch::Make(schema, count, {id_array, value_array});
}

int main() {
    std::cout << "========================================\n";
    std::cout << "Test 7.1: Throughput and Latency\n";
    std::cout << "========================================\n\n";

    std::string test_path = "/tmp/test_throughput_latency";
    std::filesystem::remove_all(test_path);

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::utf8())
    });

    // Setup database
    std::cout << "Setup: Create database...\n";
    marble::DBOptions options;
    options.db_path = test_path;
    options.enable_wal = true;

    std::unique_ptr<marble::MarbleDB> db;
    auto status = marble::MarbleDB::Open(options, nullptr, &db);
    if (!status.ok()) {
        Fail("Failed to open database: " + status.ToString());
    }

    marble::ColumnFamilyOptions cf_opts;
    cf_opts.schema = schema;

    marble::ColumnFamilyDescriptor cf_desc("perf_data", cf_opts);
    marble::ColumnFamilyHandle* cf_handle = nullptr;

    status = db->CreateColumnFamily(cf_desc, &cf_handle);
    if (!status.ok()) {
        Fail("Failed to create column family: " + status.ToString());
    }
    Pass("Database setup complete");

    // ===== Test 1: Small batch insert throughput =====
    std::cout << "\nTest 1: Small batch insert (1K records x 10 batches)...\n";
    auto start_small = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 10; i++) {
        auto batch_result = CreateBatch(i * 1000, 1000, schema);
        if (!batch_result.ok()) {
            Fail("Failed to create batch: " + batch_result.status().ToString());
        }
        status = db->InsertBatch("perf_data", *batch_result);
        if (!status.ok()) {
            Fail("Failed to insert batch: " + status.ToString());
        }
    }

    auto end_small = std::chrono::high_resolution_clock::now();
    auto small_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_small - start_small);

    double small_throughput = 10000.0 / (small_duration.count() / 1000.0);
    std::cout << "  Duration: " << small_duration.count() << " ms\n";
    std::cout << "  Throughput: " << (int)small_throughput << " records/sec\n";
    Pass("Small batch insert complete");

    // ===== Test 2: Large batch insert throughput =====
    std::cout << "\nTest 2: Large batch insert (10K records x 5 batches)...\n";
    auto start_large = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 5; i++) {
        auto batch_result = CreateBatch(10000 + i * 10000, 10000, schema);
        if (!batch_result.ok()) {
            Fail("Failed to create batch: " + batch_result.status().ToString());
        }
        status = db->InsertBatch("perf_data", *batch_result);
        if (!status.ok()) {
            Fail("Failed to insert batch: " + status.ToString());
        }
    }

    auto end_large = std::chrono::high_resolution_clock::now();
    auto large_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_large - start_large);

    double large_throughput = 50000.0 / (large_duration.count() / 1000.0);
    std::cout << "  Duration: " << large_duration.count() << " ms\n";
    std::cout << "  Throughput: " << (int)large_throughput << " records/sec\n";
    std::cout << "  Note: Larger batches should have higher throughput\n";
    Pass("Large batch insert complete");

    // ===== Test 3: Full scan throughput =====
    std::cout << "\nTest 3: Full table scan (60K records)...\n";
    auto start_scan = std::chrono::high_resolution_clock::now();

    std::unique_ptr<marble::QueryResult> scan_result;
    status = db->ScanTable("perf_data", &scan_result);
    if (!status.ok()) {
        Fail("Failed to scan: " + status.ToString());
    }

    auto table_result = QueryResultToTable(scan_result.get());
    if (!table_result.ok()) {
        Fail("Failed to convert result: " + table_result.status().ToString());
    }

    auto end_scan = std::chrono::high_resolution_clock::now();
    auto scan_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_scan - start_scan);

    auto table = *table_result;
    double scan_throughput = (double)table->num_rows() / (scan_duration.count() / 1000.0);

    std::cout << "  Rows: " << table->num_rows() << "\n";
    std::cout << "  Duration: " << scan_duration.count() << " ms\n";
    std::cout << "  Throughput: " << (int)scan_throughput << " rows/sec\n";
    Pass("Full scan complete");

    // ===== Test 4: Range query latency =====
    std::cout << "\nTest 4: Range query latency (100 queries)...\n";
    std::vector<int64_t> latencies;
    marble::ReadOptions read_opts;

    for (int i = 0; i < 100; i++) {
        int range_start = i * 500;
        int range_end = range_start + 100;

        auto key_start = std::make_shared<marble::TripleKey>(range_start, 0, 0);
        auto key_end = std::make_shared<marble::TripleKey>(range_end, 0, 0);
        marble::KeyRange range(key_start, true, key_end, false);

        auto start_range = std::chrono::high_resolution_clock::now();

        std::unique_ptr<marble::Iterator> it;
        status = db->NewIterator("perf_data", read_opts, range, &it);
        if (status.ok()) {
            it->Seek(*key_start);
            int count = 0;
            while (it->Valid() && count < 100) {
                it->Next();
                count++;
            }
        }

        auto end_range = std::chrono::high_resolution_clock::now();
        auto range_latency = std::chrono::duration_cast<std::chrono::microseconds>(
            end_range - start_range);
        latencies.push_back(range_latency.count());
    }

    // Calculate statistics
    int64_t sum = 0;
    int64_t min_lat = latencies[0];
    int64_t max_lat = latencies[0];

    for (auto lat : latencies) {
        sum += lat;
        if (lat < min_lat) min_lat = lat;
        if (lat > max_lat) max_lat = lat;
    }

    int64_t avg_lat = sum / latencies.size();

    // Calculate p50, p95, p99
    std::sort(latencies.begin(), latencies.end());
    int64_t p50 = latencies[latencies.size() * 50 / 100];
    int64_t p95 = latencies[latencies.size() * 95 / 100];
    int64_t p99 = latencies[latencies.size() * 99 / 100];

    std::cout << "  Queries: " << latencies.size() << "\n";
    std::cout << "  Average: " << avg_lat << " µs\n";
    std::cout << "  Min: " << min_lat << " µs\n";
    std::cout << "  Max: " << max_lat << " µs\n";
    std::cout << "  P50: " << p50 << " µs\n";
    std::cout << "  P95: " << p95 << " µs\n";
    std::cout << "  P99: " << p99 << " µs\n";
    Pass("Range query latency measured");

    // ===== Test 5: Random access pattern =====
    std::cout << "\nTest 5: Random point lookups (1000 lookups)...\n";
    std::mt19937 rng(12345);
    std::uniform_int_distribution<int> dist(0, 59999);

    auto start_random = std::chrono::high_resolution_clock::now();
    int found_count = 0;

    for (int i = 0; i < 1000; i++) {
        int random_key = dist(rng);
        auto key_start = std::make_shared<marble::TripleKey>(random_key, 0, 0);
        auto key_end = std::make_shared<marble::TripleKey>(random_key + 1, 0, 0);
        marble::KeyRange range(key_start, true, key_end, false);

        std::unique_ptr<marble::Iterator> it;
        status = db->NewIterator("perf_data", read_opts, range, &it);
        if (status.ok()) {
            it->Seek(*key_start);
            if (it->Valid()) {
                found_count++;
            }
        }
    }

    auto end_random = std::chrono::high_resolution_clock::now();
    auto random_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_random - start_random);

    double avg_lookup = (double)random_duration.count() * 1000.0 / 1000;
    std::cout << "  Lookups: 1000\n";
    std::cout << "  Found: " << found_count << "\n";
    std::cout << "  Duration: " << random_duration.count() << " ms\n";
    std::cout << "  Average: " << avg_lookup << " µs/lookup\n";
    Pass("Random access pattern complete");

    // ===== Test 6: Sequential vs random scan comparison =====
    std::cout << "\nTest 6: Sequential scan vs random access...\n";

    // Sequential scan
    auto start_seq = std::chrono::high_resolution_clock::now();
    std::unique_ptr<marble::QueryResult> seq_result;
    status = db->ScanTable("perf_data", &seq_result);
    if (status.ok()) {
        auto seq_table = QueryResultToTable(seq_result.get());
    }
    auto end_seq = std::chrono::high_resolution_clock::now();
    auto seq_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_seq - start_seq);

    std::cout << "  Sequential scan: " << seq_duration.count() << " ms\n";
    std::cout << "  Random lookups: " << random_duration.count() << " ms (for 1000 points)\n";
    std::cout << "  Note: Sequential should be more efficient\n";
    Pass("Sequential vs random comparison complete");

    db.reset();

    std::cout << "\n========================================\n";
    std::cout << "✅ ALL TESTS PASSED\n";
    std::cout << "========================================\n";
    std::cout << "\nPerformance Summary:\n";
    std::cout << "  Small batch: " << (int)small_throughput << " rec/sec\n";
    std::cout << "  Large batch: " << (int)large_throughput << " rec/sec\n";
    std::cout << "  Scan: " << (int)scan_throughput << " rows/sec\n";
    std::cout << "  Range query: " << avg_lat << " µs avg\n";
    std::cout << "  Point lookup: " << (int)avg_lookup << " µs avg\n";

    return 0;
}
